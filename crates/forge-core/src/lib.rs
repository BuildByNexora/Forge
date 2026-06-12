mod log;
mod parse;

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration as StdDuration;

use chrono::{DateTime, Utc};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

use log::AppendOnlyLog;
pub use parse::parse_delay;

const SNAPSHOT_FORMAT_VERSION: u32 = 1;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug, Error)]
pub enum ForgeError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid duration: {0}")]
    InvalidDuration(String),
    #[error("data directory is already locked: {path}")]
    DataDirLocked { path: String },
    #[error("unsupported snapshot format version: {0}")]
    UnsupportedSnapshot(u32),
    #[error("corrupt log at line {line}: {source}")]
    CorruptLog {
        line: usize,
        source: serde_json::Error,
    },
    #[error("storage integrity error: {0}")]
    StorageIntegrity(String),
    #[error("job not found: {0}")]
    JobNotFound(String),
    #[error("queue is empty")]
    QueueEmpty,
}

// ---------------------------------------------------------------------------
// Job status
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Queued,
    Claimed,
    Succeeded,
    Failed,
    Retrying,
    Dead,
}

impl JobStatus {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            JobStatus::Succeeded | JobStatus::Failed | JobStatus::Dead
        )
    }
}

// ---------------------------------------------------------------------------
// Job
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: String,
    pub queue: String,
    pub payload: String,
    pub priority: i64,
    pub created_at: DateTime<Utc>,
    pub scheduled_at: DateTime<Utc>,
    pub max_attempts: u32,
    pub attempt: u32,
    pub status: JobStatus,
    pub last_error: Option<String>,
    pub claimed_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
}

// ---------------------------------------------------------------------------
// History entry
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryEntry {
    pub job_id: String,
    pub status: JobStatus,
    pub at: DateTime<Utc>,
    pub error: Option<String>,
    pub attempt: u32,
}

// ---------------------------------------------------------------------------
// Events (AOF)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Event {
    JobPushed {
        job_id: String,
        queue: String,
        payload: String,
        priority: i64,
        delay_seconds: u64,
        max_attempts: u32,
        created_at: DateTime<Utc>,
    },
    JobClaimed {
        job_id: String,
        claimed_at: DateTime<Utc>,
    },
    JobSucceeded {
        job_id: String,
        completed_at: DateTime<Utc>,
    },
    JobFailed {
        job_id: String,
        error: String,
        failed_at: DateTime<Utc>,
    },
    JobRetrying {
        job_id: String,
        attempt: u32,
        next_attempt_at: DateTime<Utc>,
        error: String,
    },
    JobDead {
        job_id: String,
        error: String,
        dead_at: DateTime<Utc>,
    },
    JobRequeuedAfterCrash {
        job_id: String,
        requeued_at: DateTime<Utc>,
    },
}

#[allow(dead_code)]
impl Event {
    fn job_id(&self) -> &str {
        match self {
            Event::JobPushed { job_id, .. }
            | Event::JobClaimed { job_id, .. }
            | Event::JobSucceeded { job_id, .. }
            | Event::JobFailed { job_id, .. }
            | Event::JobRetrying { job_id, .. }
            | Event::JobDead { job_id, .. }
            | Event::JobRequeuedAfterCrash { job_id, .. } => job_id,
        }
    }
}

// ---------------------------------------------------------------------------
// Pending-job heap entry
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct PendingJob {
    job_id: String,
    priority: i64,
    scheduled_at: DateTime<Utc>,
}

impl Ord for PendingJob {
    fn cmp(&self, other: &Self) -> Ordering {
        self.priority
            .cmp(&other.priority)
            .then(other.scheduled_at.cmp(&self.scheduled_at))
    }
}

impl PartialOrd for PendingJob {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for PendingJob {
    fn eq(&self, other: &Self) -> bool {
        self.job_id == other.job_id
    }
}

impl Eq for PendingJob {}

// ---------------------------------------------------------------------------
// Snapshots
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Snapshot {
    format_version: u32,
    created_at: DateTime<Utc>,
    aof_offset: u64,
    jobs: HashMap<String, Job>,
    history: HashMap<String, Vec<HistoryEntry>>,
    #[serde(default)]
    dead_jobs: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SnapshotEnvelope {
    format_version: u32,
    created_at: DateTime<Utc>,
    checksum: String,
    snapshot: String,
}

// ---------------------------------------------------------------------------
// Sync mode
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
pub enum SyncMode {
    Always,
    Batch {
        flush_every_events: u64,
        flush_every_ms: u64,
    },
}

impl SyncMode {
    fn into_policy(self) -> Result<log::SyncPolicy, ForgeError> {
        match self {
            Self::Always => Ok(log::SyncPolicy::Always),
            Self::Batch {
                flush_every_events,
                flush_every_ms,
            } => {
                if flush_every_events == 0 {
                    return Err(ForgeError::InvalidDuration(
                        "flush_every_events must be greater than zero".into(),
                    ));
                }
                if flush_every_ms == 0 {
                    return Err(ForgeError::InvalidDuration(
                        "flush_every_ms must be greater than zero".into(),
                    ));
                }
                Ok(log::SyncPolicy::Batch {
                    flush_every_events,
                    flush_every: StdDuration::from_millis(flush_every_ms),
                })
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Doctor report
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DoctorReport {
    pub ok: bool,
    pub total_jobs: usize,
    pub queued: usize,
    pub claimed: usize,
    pub succeeded: usize,
    pub failed: usize,
    pub retrying: usize,
    pub dead: usize,
    pub history_events: usize,
    pub aof_bytes: u64,
    pub snapshot_exists: bool,
}

// ---------------------------------------------------------------------------
// Queue inner state (single Mutex for lock-ordering correctness)
// ---------------------------------------------------------------------------

struct QueueInner {
    log: AppendOnlyLog,
    jobs: HashMap<String, Job>,
    pending: BinaryHeap<PendingJob>,
    history: HashMap<String, Vec<HistoryEntry>>,
    dead_jobs: Vec<String>,
}

// ---------------------------------------------------------------------------
// Queue — the core engine
// ---------------------------------------------------------------------------

pub struct Queue {
    data_dir: PathBuf,
    inner: Mutex<QueueInner>,
    _lock_file: File,
}

impl Queue {
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self, ForgeError> {
        Self::open_with_sync(data_dir, SyncMode::Always)
    }

    pub fn open_with_sync(
        data_dir: impl AsRef<Path>,
        sync_mode: SyncMode,
    ) -> Result<Self, ForgeError> {
        let data_dir = data_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&data_dir)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&data_dir, std::fs::Permissions::from_mode(0o700))?;
        }

        let lock_path = data_dir.join("forge.lock");
        #[cfg(unix)]
        let lock_file = {
            use std::os::unix::fs::OpenOptionsExt;
            OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .truncate(false)
                .mode(0o600)
                .open(&lock_path)?
        };
        #[cfg(not(unix))]
        let lock_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)?;
        lock_file
            .try_lock_exclusive()
            .map_err(|_| ForgeError::DataDirLocked {
                path: lock_path.display().to_string(),
            })?;

        let mut log =
            AppendOnlyLog::open_with_sync(data_dir.join("forge.aof"), sync_mode.into_policy()?)?;

        // Recover from snapshot + AOF
        let (mut jobs, mut history, mut dead_jobs, offset) =
            read_snapshot(&data_dir)?.unwrap_or_default();
        let events = log.replay_from(offset)?;
        let mut pending = BinaryHeap::new();

        for event in &events {
            apply_event(&mut jobs, &mut history, &mut pending, &mut dead_jobs, event);
        }

        // Crash recovery: re-queue claimed-but-incomplete jobs
        let now = Utc::now();
        let claimed_ids: Vec<String> = jobs
            .iter()
            .filter(|(_, j)| j.status == JobStatus::Claimed)
            .map(|(id, _)| id.clone())
            .collect();

        for job_id in claimed_ids {
            if let Some(job) = jobs.get_mut(&job_id) {
                job.status = JobStatus::Queued;
                job.claimed_at = None;
                pending.push(PendingJob {
                    job_id: job.id.clone(),
                    priority: job.priority,
                    scheduled_at: now,
                });
                let ev = Event::JobRequeuedAfterCrash {
                    job_id: job.id.clone(),
                    requeued_at: now,
                };
                history
                    .entry(job.id.clone())
                    .or_default()
                    .push(HistoryEntry {
                        job_id: job.id.clone(),
                        status: JobStatus::Queued,
                        at: now,
                        error: None,
                        attempt: job.attempt,
                    });
                log.append(&ev)?;
            }
        }

        let inner = Mutex::new(QueueInner {
            log,
            jobs,
            pending,
            history,
            dead_jobs,
        });

        Ok(Self {
            data_dir,
            inner,
            _lock_file: lock_file,
        })
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub fn flush(&self) -> Result<(), ForgeError> {
        let mut inner = self.inner.lock().expect("inner lock poisoned");
        inner.log.flush()
    }

    // -----------------------------------------------------------------------
    // push
    // -----------------------------------------------------------------------

    pub fn push(
        &self,
        queue: impl Into<String>,
        payload: impl Into<String>,
        priority: i64,
        delay_seconds: u64,
    ) -> Result<String, ForgeError> {
        self.push_with_attempts(queue, payload, priority, delay_seconds, 3)
    }

    pub fn push_with_attempts(
        &self,
        queue: impl Into<String>,
        payload: impl Into<String>,
        priority: i64,
        delay_seconds: u64,
        max_attempts: u32,
    ) -> Result<String, ForgeError> {
        let job_id = Uuid::new_v4().to_string();
        let now = Utc::now();
        let queue_name = queue.into();
        let payload_str = payload.into();
        let scheduled_at = if delay_seconds > 0 {
            now + chrono::Duration::seconds(delay_seconds as i64)
        } else {
            now
        };

        let event = Event::JobPushed {
            job_id: job_id.clone(),
            queue: queue_name.clone(),
            payload: payload_str.clone(),
            priority,
            delay_seconds,
            max_attempts,
            created_at: now,
        };

        let mut inner = self.inner.lock().expect("inner lock poisoned");
        inner.log.append(&event)?;

        let job = Job {
            id: job_id.clone(),
            queue: queue_name,
            payload: payload_str,
            priority,
            created_at: now,
            scheduled_at,
            max_attempts,
            attempt: 0,
            status: JobStatus::Queued,
            last_error: None,
            claimed_at: None,
            completed_at: None,
        };

        inner
            .history
            .entry(job_id.clone())
            .or_default()
            .push(HistoryEntry {
                job_id: job_id.clone(),
                status: JobStatus::Queued,
                at: now,
                error: None,
                attempt: 0,
            });

        inner.jobs.insert(job_id.clone(), job);
        inner.pending.push(PendingJob {
            job_id: job_id.clone(),
            priority,
            scheduled_at,
        });

        Ok(job_id)
    }

    // -----------------------------------------------------------------------
    // claim — pop highest priority ready job
    // -----------------------------------------------------------------------

    pub fn claim(&self) -> Result<Job, ForgeError> {
        let now = Utc::now();
        let mut inner = self.inner.lock().expect("inner lock poisoned");

        loop {
            let ready = match inner.pending.peek() {
                Some(p) if p.scheduled_at <= now => inner.pending.pop().unwrap(),
                _ => break,
            };

            let (jid, attempt, result) = {
                let job = match inner.jobs.get_mut(&ready.job_id) {
                    Some(job) => job,
                    None => continue,
                };
                if job.status != JobStatus::Queued {
                    continue;
                }
                job.status = JobStatus::Claimed;
                job.claimed_at = Some(now);
                job.attempt = job.attempt.saturating_add(1);
                (job.id.clone(), job.attempt, job.clone())
            };

            let ev = Event::JobClaimed {
                job_id: jid.clone(),
                claimed_at: now,
            };
            inner.log.append(&ev)?;

            inner
                .history
                .entry(jid.clone())
                .or_default()
                .push(HistoryEntry {
                    job_id: jid,
                    status: JobStatus::Claimed,
                    at: now,
                    error: None,
                    attempt,
                });

            return Ok(result);
        }

        Err(ForgeError::QueueEmpty)
    }

    // -----------------------------------------------------------------------
    // acknowledge / succeed
    // -----------------------------------------------------------------------

    pub fn acknowledge(&self, job_id: &str) -> Result<(), ForgeError> {
        let now = Utc::now();
        let mut inner = self.inner.lock().expect("inner lock poisoned");

        let (jid, attempt) = {
            let job = inner
                .jobs
                .get_mut(job_id)
                .ok_or_else(|| ForgeError::JobNotFound(job_id.to_string()))?;
            if job.status != JobStatus::Claimed {
                return Err(ForgeError::JobNotFound(format!(
                    "job {job_id} is not claimed (status: {:?})",
                    job.status
                )));
            }
            job.status = JobStatus::Succeeded;
            job.completed_at = Some(now);
            (job.id.clone(), job.attempt)
        };

        let ev = Event::JobSucceeded {
            job_id: jid.clone(),
            completed_at: now,
        };
        inner.log.append(&ev)?;

        inner
            .history
            .entry(jid.clone())
            .or_default()
            .push(HistoryEntry {
                job_id: jid,
                status: JobStatus::Succeeded,
                at: now,
                error: None,
                attempt,
            });

        Ok(())
    }

    // -----------------------------------------------------------------------
    // fail — with retry or dead letter
    // -----------------------------------------------------------------------

    pub fn fail(&self, job_id: &str, error: &str) -> Result<(), ForgeError> {
        let now = Utc::now();
        let mut inner = self.inner.lock().expect("inner lock poisoned");

        let (jid, attempt, max_attempts, priority) = {
            let job = inner
                .jobs
                .get_mut(job_id)
                .ok_or_else(|| ForgeError::JobNotFound(job_id.to_string()))?;
            if job.status != JobStatus::Claimed {
                return Err(ForgeError::JobNotFound(format!(
                    "job {job_id} is not claimed (status: {:?})",
                    job.status
                )));
            }
            let jid = job.id.clone();
            let attempt = job.attempt;
            let max_attempts = job.max_attempts;
            let priority = job.priority;

            if attempt >= max_attempts {
                job.status = JobStatus::Dead;
                job.last_error = Some(error.to_string());
                job.completed_at = Some(now);
            } else {
                job.status = JobStatus::Retrying;
                job.last_error = Some(error.to_string());
            }
            (jid, attempt, max_attempts, priority)
        };

        if attempt >= max_attempts {
            let ev = Event::JobDead {
                job_id: jid.clone(),
                error: error.to_string(),
                dead_at: now,
            };
            inner.log.append(&ev)?;

            inner.dead_jobs.push(jid.clone());

            inner
                .history
                .entry(jid.clone())
                .or_default()
                .push(HistoryEntry {
                    job_id: jid,
                    status: JobStatus::Dead,
                    at: now,
                    error: Some(error.to_string()),
                    attempt,
                });
        } else {
            let backoff_seconds = 2u64.pow(attempt.saturating_sub(1)) * 10;
            let next_attempt_at = now + chrono::Duration::seconds(backoff_seconds as i64);

            let ev = Event::JobRetrying {
                job_id: jid.clone(),
                attempt,
                next_attempt_at,
                error: error.to_string(),
            };
            inner.log.append(&ev)?;

            inner
                .history
                .entry(jid.clone())
                .or_default()
                .push(HistoryEntry {
                    job_id: jid.clone(),
                    status: JobStatus::Retrying,
                    at: now,
                    error: Some(error.to_string()),
                    attempt,
                });

            let requeue_ev = Event::JobRequeuedAfterCrash {
                job_id: jid.clone(),
                requeued_at: next_attempt_at,
            };
            inner.log.append(&requeue_ev)?;

            inner.pending.push(PendingJob {
                job_id: jid.clone(),
                priority,
                scheduled_at: next_attempt_at,
            });

            inner
                .history
                .entry(jid.clone())
                .or_default()
                .push(HistoryEntry {
                    job_id: jid.clone(),
                    status: JobStatus::Queued,
                    at: next_attempt_at,
                    error: None,
                    attempt,
                });

            {
                let job = inner.jobs.get_mut(&jid).expect("job must exist for retry");
                job.status = JobStatus::Queued;
                job.claimed_at = None;
            }
        }

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Queries
    // -----------------------------------------------------------------------

    pub fn list(&self) -> Result<Vec<Job>, ForgeError> {
        let inner = self.inner.lock().expect("inner lock poisoned");
        Ok(inner.jobs.values().cloned().collect())
    }

    pub fn status(&self, job_id: &str) -> Result<Option<Job>, ForgeError> {
        let inner = self.inner.lock().expect("inner lock poisoned");
        Ok(inner.jobs.get(job_id).cloned())
    }

    pub fn history(&self, job_id: &str) -> Result<Vec<HistoryEntry>, ForgeError> {
        let inner = self.inner.lock().expect("inner lock poisoned");
        Ok(inner.history.get(job_id).cloned().unwrap_or_default())
    }

    pub fn dead_list(&self) -> Result<Vec<Job>, ForgeError> {
        let inner = self.inner.lock().expect("inner lock poisoned");
        let ids: Vec<String> = inner.dead_jobs.clone();
        Ok(ids
            .iter()
            .filter_map(|id| inner.jobs.get(id))
            .cloned()
            .collect())
    }

    pub fn dead_retry(&self, job_id: &str) -> Result<(), ForgeError> {
        let now = Utc::now();
        let mut inner = self.inner.lock().expect("inner lock poisoned");

        let (jid, priority, queue_name, payload, max_attempts) = {
            let job = inner
                .jobs
                .get_mut(job_id)
                .ok_or_else(|| ForgeError::JobNotFound(job_id.to_string()))?;

            if job.status != JobStatus::Dead {
                return Err(ForgeError::JobNotFound(format!(
                    "job {job_id} is not in dead letter queue"
                )));
            }

            job.status = JobStatus::Queued;
            job.attempt = 0;
            job.claimed_at = None;
            job.last_error = None;

            (
                job.id.clone(),
                job.priority,
                job.queue.clone(),
                job.payload.clone(),
                job.max_attempts,
            )
        };

        inner.pending.push(PendingJob {
            job_id: jid.clone(),
            priority,
            scheduled_at: now,
        });

        inner.dead_jobs.retain(|id| id != job_id);

        let event = Event::JobPushed {
            job_id: jid.clone(),
            queue: queue_name,
            payload,
            priority,
            delay_seconds: 0,
            max_attempts,
            created_at: now,
        };
        inner.log.append(&event)?;

        inner
            .history
            .entry(jid.clone())
            .or_default()
            .push(HistoryEntry {
                job_id: jid,
                status: JobStatus::Queued,
                at: now,
                error: None,
                attempt: 0,
            });

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Compact
    // -----------------------------------------------------------------------

    pub fn compact(&self) -> Result<(), ForgeError> {
        let mut inner = self.inner.lock().expect("inner lock poisoned");
        inner.log.flush()?;

        inner.log.truncate()?;
        let snapshot = Snapshot {
            format_version: SNAPSHOT_FORMAT_VERSION,
            created_at: Utc::now(),
            aof_offset: 0,
            jobs: inner.jobs.clone(),
            history: inner.history.clone(),
            dead_jobs: inner.dead_jobs.clone(),
        };
        write_snapshot(&self.data_dir, &snapshot)?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Doctor
    // -----------------------------------------------------------------------

    pub fn doctor(&self) -> Result<DoctorReport, ForgeError> {
        let snapshot_exists = self.data_dir.join("forge.snapshot").exists();
        let inner = self.inner.lock().expect("inner lock poisoned");
        let aof_bytes = inner.log.len()?;
        let queued = inner
            .jobs
            .values()
            .filter(|j| j.status == JobStatus::Queued)
            .count();
        let claimed = inner
            .jobs
            .values()
            .filter(|j| j.status == JobStatus::Claimed)
            .count();
        let succeeded = inner
            .jobs
            .values()
            .filter(|j| j.status == JobStatus::Succeeded)
            .count();
        let failed = inner
            .jobs
            .values()
            .filter(|j| j.status == JobStatus::Failed)
            .count();
        let retrying = inner
            .jobs
            .values()
            .filter(|j| j.status == JobStatus::Retrying)
            .count();
        let dead = inner
            .jobs
            .values()
            .filter(|j| j.status == JobStatus::Dead)
            .count();
        let total_jobs = inner.jobs.len();
        let history_events: usize = inner.history.values().map(|v| v.len()).sum();
        Ok(DoctorReport {
            ok: true,
            total_jobs,
            queued,
            claimed,
            succeeded,
            failed,
            retrying,
            dead,
            history_events,
            aof_bytes,
            snapshot_exists,
        })
    }
}

/// SAFETY: All internal state is behind Mutex, and the log's File is Sync.
/// The lock_file is Sync (File is Sync on all platforms).
unsafe impl Sync for Queue {}

// ---------------------------------------------------------------------------
// Event application (state machine)
// ---------------------------------------------------------------------------

fn apply_event(
    jobs: &mut HashMap<String, Job>,
    history: &mut HashMap<String, Vec<HistoryEntry>>,
    pending: &mut BinaryHeap<PendingJob>,
    dead_jobs: &mut Vec<String>,
    event: &Event,
) {
    match event.clone() {
        Event::JobPushed {
            job_id,
            queue,
            payload,
            priority,
            delay_seconds,
            max_attempts,
            created_at,
        } => {
            let scheduled_at = if delay_seconds > 0 {
                created_at + chrono::Duration::seconds(delay_seconds as i64)
            } else {
                created_at
            };
            let job = Job {
                id: job_id.clone(),
                queue,
                payload,
                priority,
                created_at,
                scheduled_at,
                max_attempts,
                attempt: 0,
                status: JobStatus::Queued,
                last_error: None,
                claimed_at: None,
                completed_at: None,
            };
            history
                .entry(job_id.clone())
                .or_default()
                .push(HistoryEntry {
                    job_id: job_id.clone(),
                    status: JobStatus::Queued,
                    at: created_at,
                    error: None,
                    attempt: 0,
                });
            pending.push(PendingJob {
                job_id: job_id.clone(),
                priority,
                scheduled_at,
            });
            jobs.insert(job_id, job);
        }
        Event::JobClaimed {
            job_id, claimed_at, ..
        } => {
            if let Some(job) = jobs.get_mut(&job_id) {
                job.status = JobStatus::Claimed;
                job.claimed_at = Some(claimed_at);
                job.attempt = job.attempt.saturating_add(1);
                history
                    .entry(job_id.clone())
                    .or_default()
                    .push(HistoryEntry {
                        job_id: job_id.clone(),
                        status: JobStatus::Claimed,
                        at: claimed_at,
                        error: None,
                        attempt: job.attempt,
                    });
            }
        }
        Event::JobSucceeded {
            job_id,
            completed_at,
            ..
        } => {
            if let Some(job) = jobs.get_mut(&job_id) {
                job.status = JobStatus::Succeeded;
                job.completed_at = Some(completed_at);
                history
                    .entry(job_id.clone())
                    .or_default()
                    .push(HistoryEntry {
                        job_id: job_id.clone(),
                        status: JobStatus::Succeeded,
                        at: completed_at,
                        error: None,
                        attempt: job.attempt,
                    });
            }
        }
        Event::JobFailed {
            job_id,
            error,
            failed_at,
        } => {
            if let Some(job) = jobs.get_mut(&job_id) {
                job.status = JobStatus::Failed;
                job.last_error = Some(error.clone());
                job.completed_at = Some(failed_at);
                history
                    .entry(job_id.clone())
                    .or_default()
                    .push(HistoryEntry {
                        job_id: job_id.clone(),
                        status: JobStatus::Failed,
                        at: failed_at,
                        error: Some(error),
                        attempt: job.attempt,
                    });
            }
        }
        Event::JobRetrying {
            job_id,
            attempt,
            next_attempt_at,
            error,
        } => {
            if let Some(job) = jobs.get_mut(&job_id) {
                job.status = JobStatus::Retrying;
                job.last_error = Some(error.clone());
                history
                    .entry(job_id.clone())
                    .or_default()
                    .push(HistoryEntry {
                        job_id: job_id.clone(),
                        status: JobStatus::Retrying,
                        at: next_attempt_at,
                        error: Some(error),
                        attempt,
                    });
            }
        }
        Event::JobDead {
            job_id,
            error,
            dead_at,
        } => {
            if let Some(job) = jobs.get_mut(&job_id) {
                job.status = JobStatus::Dead;
                job.last_error = Some(error.clone());
                job.completed_at = Some(dead_at);
                dead_jobs.push(job_id.clone());
                history
                    .entry(job_id.clone())
                    .or_default()
                    .push(HistoryEntry {
                        job_id: job_id.clone(),
                        status: JobStatus::Dead,
                        at: dead_at,
                        error: Some(error),
                        attempt: job.attempt,
                    });
            }
        }
        Event::JobRequeuedAfterCrash {
            job_id,
            requeued_at,
        } => {
            if let Some(job) = jobs.get_mut(&job_id) {
                job.status = JobStatus::Queued;
                job.claimed_at = None;
                pending.push(PendingJob {
                    job_id: job_id.clone(),
                    priority: job.priority,
                    scheduled_at: requeued_at,
                });
                history
                    .entry(job_id.clone())
                    .or_default()
                    .push(HistoryEntry {
                        job_id: job_id.clone(),
                        status: JobStatus::Queued,
                        at: requeued_at,
                        error: None,
                        attempt: job.attempt,
                    });
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Snapshot I/O
// ---------------------------------------------------------------------------

fn snapshot_path(data_dir: &Path) -> PathBuf {
    data_dir.join("forge.snapshot")
}

type SnapshotData = (
    HashMap<String, Job>,
    HashMap<String, Vec<HistoryEntry>>,
    Vec<String>,
    u64,
);

fn read_snapshot(data_dir: &Path) -> Result<Option<SnapshotData>, ForgeError> {
    let path = snapshot_path(data_dir);
    if !path.exists() {
        return Ok(None);
    }
    let bytes = std::fs::read(path)?;
    let value: serde_json::Value = serde_json::from_slice(&bytes)?;
    let snapshot: Snapshot = if looks_like_snapshot_envelope(&value) {
        let envelope: SnapshotEnvelope = serde_json::from_value(value)?;
        if envelope.format_version != SNAPSHOT_FORMAT_VERSION {
            return Err(ForgeError::UnsupportedSnapshot(envelope.format_version));
        }
        verify_snapshot_checksum(&envelope)?;
        serde_json::from_str(&envelope.snapshot)?
    } else {
        serde_json::from_value(value)?
    };
    if snapshot.format_version != SNAPSHOT_FORMAT_VERSION {
        return Err(ForgeError::UnsupportedSnapshot(snapshot.format_version));
    }
    Ok(Some((
        snapshot.jobs,
        snapshot.history,
        snapshot.dead_jobs,
        snapshot.aof_offset,
    )))
}

fn looks_like_snapshot_envelope(value: &serde_json::Value) -> bool {
    value
        .as_object()
        .map(|object| object.contains_key("checksum") || object.contains_key("snapshot"))
        .unwrap_or(false)
}

fn write_snapshot(data_dir: &Path, snapshot: &Snapshot) -> Result<(), ForgeError> {
    let tmp = data_dir.join("forge.snapshot.tmp");
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&tmp)?;
    let snapshot_json = canonical_snapshot_json(snapshot)?;
    let envelope = SnapshotEnvelope {
        format_version: SNAPSHOT_FORMAT_VERSION,
        created_at: Utc::now(),
        checksum: snapshot_string_checksum(&snapshot_json),
        snapshot: snapshot_json,
    };
    serde_json::to_writer_pretty(&mut file, &envelope)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    std::fs::rename(&tmp, snapshot_path(data_dir))?;
    File::open(data_dir)?.sync_all()?;
    Ok(())
}

fn verify_snapshot_checksum(envelope: &SnapshotEnvelope) -> Result<(), ForgeError> {
    let actual = snapshot_string_checksum(&envelope.snapshot);
    if !constant_time_eq(envelope.checksum.as_bytes(), actual.as_bytes()) {
        return Err(ForgeError::StorageIntegrity(format!(
            "snapshot checksum mismatch: expected {}, got {}",
            envelope.checksum, actual
        )));
    }
    Ok(())
}

fn canonical_snapshot_json(snapshot: &Snapshot) -> Result<String, ForgeError> {
    let mut value = serde_json::to_value(snapshot)?;
    canonicalize_json(&mut value);
    Ok(serde_json::to_string(&value)?)
}

fn snapshot_string_checksum(value: &str) -> String {
    hex_sha256(value.as_bytes())
}

fn canonicalize_json(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Array(values) => {
            for value in values {
                canonicalize_json(value);
            }
        }
        serde_json::Value::Object(values) => {
            let mut sorted = values
                .iter_mut()
                .map(|(key, value)| {
                    canonicalize_json(value);
                    (key.clone(), value.take())
                })
                .collect::<Vec<_>>();
            sorted.sort_by(|left, right| left.0.cmp(&right.0));
            values.clear();
            for (key, value) in sorted {
                values.insert(key, value);
            }
        }
        _ => {}
    }
}

fn hex_sha256(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(out, "{byte:02x}");
    }
    out
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |acc, (left, right)| acc | (left ^ right))
        == 0
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use tempfile::TempDir;

    #[test]
    fn push_and_claim_round_trip() {
        let dir = TempDir::new().unwrap();
        let queue = Queue::open(dir.path()).unwrap();
        let id = queue.push("emails", r#"{"to":"a@b.com"}"#, 1, 0).unwrap();
        let job = queue.claim().unwrap();
        assert_eq!(job.id, id);
        assert_eq!(job.queue, "emails");
        assert_eq!(job.status, JobStatus::Claimed);
    }

    #[test]
    fn claim_empty_returns_error() {
        let dir = TempDir::new().unwrap();
        let queue = Queue::open(dir.path()).unwrap();
        match queue.claim() {
            Err(ForgeError::QueueEmpty) => {}
            other => panic!("expected QueueEmpty, got {other:?}"),
        }
    }

    #[test]
    fn acknowledge_marks_job_succeeded() {
        let dir = TempDir::new().unwrap();
        let queue = Queue::open(dir.path()).unwrap();
        let id = queue.push("test", r#"{}"#, 0, 0).unwrap();
        let _job = queue.claim().unwrap();
        queue.acknowledge(&id).unwrap();
        let job = queue.status(&id).unwrap().unwrap();
        assert_eq!(job.status, JobStatus::Succeeded);
    }

    #[test]
    fn fail_moves_to_dead_after_max_attempts() {
        let dir = TempDir::new().unwrap();
        let queue = Queue::open(dir.path()).unwrap();
        let id = queue.push_with_attempts("test", r#"{}"#, 0, 0, 1).unwrap();
        let _job = queue.claim().unwrap();
        queue.fail(&id, "it broke").unwrap();
        let job = queue.status(&id).unwrap().unwrap();
        assert_eq!(job.status, JobStatus::Dead);
        let dead = queue.dead_list().unwrap();
        assert_eq!(dead.len(), 1);
    }

    #[test]
    fn fail_retries_with_backoff() {
        let dir = TempDir::new().unwrap();
        let queue = Queue::open(dir.path()).unwrap();
        let id = queue.push_with_attempts("test", r#"{}"#, 0, 0, 3).unwrap();
        let _job = queue.claim().unwrap();
        queue.fail(&id, "will retry").unwrap();
        let job = queue.status(&id).unwrap().unwrap();
        assert_eq!(job.status, JobStatus::Queued);
        assert_eq!(job.attempt, 1);
    }

    #[test]
    fn priority_ordering() {
        let dir = TempDir::new().unwrap();
        let queue = Queue::open(dir.path()).unwrap();
        queue.push("q", "low", 0, 0).unwrap();
        queue.push("q", "high", 10, 0).unwrap();
        queue.push("q", "mid", 5, 0).unwrap();

        let job = queue.claim().unwrap();
        assert_eq!(job.payload, "high");
        let job = queue.claim().unwrap();
        assert_eq!(job.payload, "mid");
        let job = queue.claim().unwrap();
        assert_eq!(job.payload, "low");
    }

    #[test]
    fn delay_holds_job_until_ready() {
        let dir = TempDir::new().unwrap();
        let queue = Queue::open(dir.path()).unwrap();
        queue.push("q", r#"{}"#, 0, 3600).unwrap(); // 1 hour delay
        match queue.claim() {
            Err(ForgeError::QueueEmpty) => {}
            other => panic!("expected QueueEmpty, got {other:?}"),
        }
    }

    #[test]
    fn history_tracks_job_lifecycle() {
        let dir = TempDir::new().unwrap();
        let queue = Queue::open(dir.path()).unwrap();
        let id = queue.push("test", r#"{}"#, 0, 0).unwrap();
        let _job = queue.claim().unwrap();
        queue.acknowledge(&id).unwrap();
        let history = queue.history(&id).unwrap();
        assert_eq!(history.len(), 3);
        assert_eq!(history[0].status, JobStatus::Queued);
        assert_eq!(history[1].status, JobStatus::Claimed);
        assert_eq!(history[2].status, JobStatus::Succeeded);
    }

    #[test]
    fn crash_recovery_requeues_claimed_jobs() {
        let dir = TempDir::new().unwrap();
        let id = {
            let queue = Queue::open(dir.path()).unwrap();
            let id = queue.push("test", r#"{}"#, 0, 0).unwrap();
            let _job = queue.claim().unwrap();
            // drop without acknowledge -> simulates crash
            id
        };

        let queue = Queue::open(dir.path()).unwrap();
        let job = queue.status(&id).unwrap().unwrap();
        assert_eq!(job.status, JobStatus::Queued);
    }

    #[test]
    fn data_dir_lock_is_exclusive() {
        let dir = TempDir::new().unwrap();
        let queue = Queue::open(dir.path()).unwrap();
        match Queue::open(dir.path()) {
            Err(ForgeError::DataDirLocked { .. }) => {}
            Ok(_) => panic!("second queue unexpectedly acquired lock"),
            Err(err) => panic!("unexpected error: {err}"),
        }
        drop(queue);
        Queue::open(dir.path()).unwrap();
    }

    #[test]
    fn compaction_preserves_state() {
        let dir = TempDir::new().unwrap();
        let queue = Queue::open(dir.path()).unwrap();
        let id = queue
            .push_with_attempts("test", r#"{"x":1}"#, 5, 0, 3)
            .unwrap();
        let _job = queue.claim().unwrap();
        queue.acknowledge(&id).unwrap();
        queue.compact().unwrap();
        drop(queue);

        let queue = Queue::open(dir.path()).unwrap();
        let job = queue.status(&id).unwrap().unwrap();
        assert_eq!(job.status, JobStatus::Succeeded);
        assert_eq!(job.payload, r#"{"x":1}"#);
    }

    #[test]
    fn dead_retry_requeues_job() {
        let dir = TempDir::new().unwrap();
        let queue = Queue::open(dir.path()).unwrap();
        let id = queue.push_with_attempts("test", r#"{}"#, 0, 0, 1).unwrap();
        let _job = queue.claim().unwrap();
        queue.fail(&id, "dead").unwrap();
        assert_eq!(queue.dead_list().unwrap().len(), 1);

        queue.dead_retry(&id).unwrap();
        let job = queue.status(&id).unwrap().unwrap();
        assert_eq!(job.status, JobStatus::Queued);
        assert_eq!(queue.dead_list().unwrap().len(), 0);
    }

    #[test]
    fn doctor_returns_report() {
        let dir = TempDir::new().unwrap();
        let queue = Queue::open(dir.path()).unwrap();
        queue.push("q", "p", 0, 0).unwrap();
        let report = queue.doctor().unwrap();
        assert!(report.ok);
        assert!(report.aof_bytes > 0);
    }

    #[test]
    fn acknowledge_rejects_non_claimed() {
        let dir = TempDir::new().unwrap();
        let queue = Queue::open(dir.path()).unwrap();
        let id = queue.push("t", "{}", 0, 0).unwrap();
        // Can't ack a job that hasn't been claimed
        match queue.acknowledge(&id) {
            Err(ForgeError::JobNotFound(_)) => {}
            other => panic!("expected JobNotFound, got {other:?}"),
        }
        // Can't fail a job that hasn't been claimed
        match queue.fail(&id, "nope") {
            Err(ForgeError::JobNotFound(_)) => {}
            other => panic!("expected JobNotFound, got {other:?}"),
        }
    }

    #[test]
    fn dead_list_survives_compaction() {
        let dir = TempDir::new().unwrap();
        let queue = Queue::open(dir.path()).unwrap();
        let id = queue.push_with_attempts("t", "{}", 0, 0, 1).unwrap();
        let _job = queue.claim().unwrap();
        queue.fail(&id, "dead").unwrap();
        assert_eq!(queue.dead_list().unwrap().len(), 1);
        queue.compact().unwrap();
        assert_eq!(
            queue.dead_list().unwrap().len(),
            1,
            "dead list must survive compaction"
        );
        drop(queue);
        let queue = Queue::open(dir.path()).unwrap();
        assert_eq!(
            queue.dead_list().unwrap().len(),
            1,
            "dead list must survive reopen from snapshot"
        );
    }

    #[test]
    fn concurrent_push_and_claim() {
        let dir = TempDir::new().unwrap();
        let queue = Arc::new(Queue::open(dir.path()).unwrap());
        let mut handles = Vec::new();
        for i in 0..10 {
            let q = Arc::clone(&queue);
            handles.push(thread::spawn(move || {
                q.push("concurrent", format!("job-{i}"), 0, 0).unwrap();
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        let mut claimed = 0;
        loop {
            match queue.claim() {
                Ok(job) => {
                    queue.acknowledge(&job.id).unwrap();
                    claimed += 1;
                }
                Err(ForgeError::QueueEmpty) => break,
                Err(e) => panic!("{e}"),
            }
        }
        assert_eq!(claimed, 10);
    }
}
