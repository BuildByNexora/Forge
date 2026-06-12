#![allow(clippy::useless_conversion)]
#![allow(unexpected_cfgs)]

use std::sync::Arc;

use forge_core::{ForgeError, JobStatus, Queue, SyncMode};
use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use std::sync::mpsc;
use std::sync::Mutex;
use std::thread::{self, JoinHandle};

create_exception!(forge, PyForgeError, PyException);

// ---------------------------------------------------------------------------
// Native Queue binding
// ---------------------------------------------------------------------------

#[pyclass]
struct NativeQueue {
    inner: Arc<Queue>,
}

#[pymethods]
impl NativeQueue {
    #[new]
    #[pyo3(signature = (data_dir=".forge", *, sync="always", flush_every_ms=100, flush_every_events=100))]
    fn new(
        data_dir: &str,
        sync: &str,
        flush_every_ms: u64,
        flush_every_events: u64,
    ) -> PyResult<Self> {
        let sync_mode = parse_sync_mode(sync, flush_every_ms, flush_every_events)?;
        Ok(Self {
            inner: Arc::new(Queue::open_with_sync(data_dir, sync_mode).map_err(py_err)?),
        })
    }

    #[pyo3(signature = (queue_name, payload, *, priority=0, delay=None, max_attempts=3))]
    fn push(
        &self,
        py: Python<'_>,
        queue_name: String,
        payload: String,
        priority: i64,
        delay: Option<String>,
        max_attempts: u32,
    ) -> PyResult<String> {
        let delay_seconds = match delay {
            Some(d) => forge_core::parse_delay(&d).map_err(py_err)?,
            None => 0,
        };
        py.allow_threads(|| {
            self.inner.push_with_attempts(
                queue_name,
                payload,
                priority,
                delay_seconds,
                max_attempts,
            )
        })
        .map_err(py_err)
    }

    fn claim(&self, py: Python<'_>) -> PyResult<Option<PyObject>> {
        let result = py.allow_threads(|| self.inner.claim());
        match result {
            Ok(job) => Ok(Some(job_to_py(py, job)?)),
            Err(ForgeError::QueueEmpty) => Ok(None),
            Err(err) => Err(py_err(err)),
        }
    }

    fn acknowledge(&self, py: Python<'_>, job_id: String) -> PyResult<()> {
        py.allow_threads(|| self.inner.acknowledge(&job_id))
            .map_err(py_err)
    }

    #[pyo3(signature = (job_id, error="unknown error"))]
    fn fail(&self, py: Python<'_>, job_id: String, error: &str) -> PyResult<()> {
        py.allow_threads(|| self.inner.fail(&job_id, error))
            .map_err(py_err)
    }

    fn status(&self, py: Python<'_>, job_id: String) -> PyResult<Option<PyObject>> {
        let result = py
            .allow_threads(|| self.inner.status(&job_id))
            .map_err(py_err)?;
        match result {
            Some(job) => Ok(Some(job_to_py(py, job)?)),
            None => Ok(None),
        }
    }

    fn list(&self, py: Python<'_>) -> PyResult<PyObject> {
        let jobs = py.allow_threads(|| self.inner.list()).map_err(py_err)?;
        let out = PyList::empty_bound(py);
        for job in jobs {
            out.append(job_to_py(py, job)?)?;
        }
        Ok(out.into_py(py))
    }

    fn history(&self, py: Python<'_>, job_id: String) -> PyResult<PyObject> {
        let entries = py
            .allow_threads(|| self.inner.history(&job_id))
            .map_err(py_err)?;
        let value = serde_json::to_value(entries)
            .map_err(|err| PyRuntimeError::new_err(err.to_string()))?;
        json_to_py(py, value)
    }

    fn dead_list(&self, py: Python<'_>) -> PyResult<PyObject> {
        let jobs = py
            .allow_threads(|| self.inner.dead_list())
            .map_err(py_err)?;
        let out = PyList::empty_bound(py);
        for job in jobs {
            out.append(job_to_py(py, job)?)?;
        }
        Ok(out.into_py(py))
    }

    fn dead_retry(&self, py: Python<'_>, job_id: String) -> PyResult<()> {
        py.allow_threads(|| self.inner.dead_retry(&job_id))
            .map_err(py_err)
    }

    fn compact(&self, py: Python<'_>) -> PyResult<()> {
        py.allow_threads(|| self.inner.compact()).map_err(py_err)
    }

    fn flush(&self, py: Python<'_>) -> PyResult<()> {
        py.allow_threads(|| self.inner.flush()).map_err(py_err)
    }

    fn doctor(&self, py: Python<'_>) -> PyResult<PyObject> {
        let report = py.allow_threads(|| self.inner.doctor()).map_err(py_err)?;
        json_to_py(
            py,
            serde_json::to_value(report).map_err(|err| PyRuntimeError::new_err(err.to_string()))?,
        )
    }
}

// ---------------------------------------------------------------------------
// Context object passed to Python handlers
// ---------------------------------------------------------------------------

#[pyclass]
#[pyo3(get_all)]
struct PyContext {
    job_id: String,
    attempt: u32,
}

// ---------------------------------------------------------------------------
// Handler storage — shared between WorkerRuntime and its background thread
// ---------------------------------------------------------------------------

struct HandlerReg {
    queue_name: String,
    callable: Py<PyAny>,
}

// ---------------------------------------------------------------------------
// Worker runtime for Python (blocking run & background thread start)
// ---------------------------------------------------------------------------

#[pyclass]
struct WorkerRuntime {
    queue: Arc<Queue>,
    handlers: Arc<Mutex<Vec<HandlerReg>>>,
    stop_tx: Option<mpsc::Sender<()>>,
    join: Option<JoinHandle<()>>,
}

#[pymethods]
impl WorkerRuntime {
    #[new]
    fn new(data_dir: &str) -> PyResult<Self> {
        Ok(Self {
            queue: Arc::new(Queue::open(data_dir).map_err(py_err)?),
            handlers: Arc::new(Mutex::new(Vec::new())),
            stop_tx: None,
            join: None,
        })
    }

    fn attach(&mut self, _py: Python<'_>, queue: &NativeQueue) {
        self.queue = Arc::clone(&queue.inner);
    }

    #[pyo3(signature = (name))]
    fn worker(&self, name: &str) -> PyResult<PyObject> {
        Python::with_gil(|py| {
            let decorator = PyWorkerDecorator {
                handlers: Arc::clone(&self.handlers),
                name: name.to_string(),
            };
            Py::new(py, decorator).map(|obj| obj.into_py(py))
        })
    }

    fn run(&self, py: Python<'_>) -> PyResult<()> {
        loop {
            let job = match py.allow_threads(|| self.queue.claim()) {
                Ok(job) => job,
                Err(ForgeError::QueueEmpty) => {
                    thread::sleep(std::time::Duration::from_millis(100));
                    continue;
                }
                Err(err) => return Err(py_err(err)),
            };

            self.handle_job(py, job)?;
        }
    }

    fn start(&mut self, _py: Python<'_>) -> PyResult<()> {
        let (stop_tx, stop_rx) = mpsc::channel();
        let handlers = Arc::clone(&self.handlers);
        let queue = Arc::clone(&self.queue);

        let join = thread::spawn(move || loop {
            if stop_rx.try_recv().is_ok() {
                break;
            }

            let job = match queue.claim() {
                Ok(job) => job,
                Err(ForgeError::QueueEmpty) => {
                    thread::sleep(std::time::Duration::from_millis(100));
                    continue;
                }
                Err(_) => break,
            };

            Python::with_gil(|py| {
                let _ = dispatch_job(py, &queue, &handlers, job);
            });
        });

        self.stop_tx = Some(stop_tx);
        self.join = Some(join);
        Ok(())
    }

    fn stop(&mut self) -> PyResult<()> {
        if let Some(tx) = self.stop_tx.take() {
            let _ = tx.send(());
        }
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
        Ok(())
    }
}

impl WorkerRuntime {
    fn handle_job(&self, py: Python<'_>, job: forge_core::Job) -> PyResult<()> {
        dispatch_job(py, &self.queue, &self.handlers, job)
    }
}

impl Drop for WorkerRuntime {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

fn dispatch_job(
    py: Python<'_>,
    queue: &Queue,
    handlers: &Mutex<Vec<HandlerReg>>,
    job: forge_core::Job,
) -> PyResult<()> {
    let callable = {
        let guard = handlers.lock().expect("handlers lock poisoned");
        guard
            .iter()
            .find(|h| h.queue_name == job.queue)
            .map(|h| h.callable.clone_ref(py))
    };

    let Some(callable) = callable else {
        py.allow_threads(|| queue.acknowledge(&job.id))
            .map_err(py_err)?;
        return Ok(());
    };

    let payload: PyObject = {
        let json = py.import_bound("json").ok();
        match json.and_then(|j| j.call_method1("loads", (job.payload.clone(),)).ok()) {
            Some(val) => val.unbind(),
            None => job.payload.clone().into_py(py),
        }
    };

    let ctx_obj = Py::new(
        py,
        PyContext {
            job_id: job.id.clone(),
            attempt: job.attempt,
        },
    )
    .map_err(|err| PyRuntimeError::new_err(format!("failed to create context: {err}")))?;

    let result = callable.bind(py).call1((payload, ctx_obj));

    match result {
        Ok(_) => py
            .allow_threads(|| queue.acknowledge(&job.id))
            .map_err(py_err),
        Err(err) => {
            let error_text = err.to_string();
            py.allow_threads(|| queue.fail(&job.id, &error_text))
                .map_err(py_err)
        }
    }
}

// ---------------------------------------------------------------------------
// Worker decorator class
// ---------------------------------------------------------------------------

#[pyclass]
struct PyWorkerDecorator {
    handlers: Arc<Mutex<Vec<HandlerReg>>>,
    name: String,
}

#[pymethods]
impl PyWorkerDecorator {
    fn __call__(&self, callable: Py<PyAny>) -> PyResult<Py<PyAny>> {
        Python::with_gil(|py| {
            let mut handlers = self.handlers.lock().expect("handlers lock poisoned");
            handlers.push(HandlerReg {
                queue_name: self.name.clone(),
                callable: callable.clone_ref(py),
            });
        });
        Ok(callable)
    }
}

// ---------------------------------------------------------------------------
// Module definition
// ---------------------------------------------------------------------------

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<NativeQueue>()?;
    m.add_class::<WorkerRuntime>()?;
    m.add("ForgeError", m.py().get_type_bound::<PyForgeError>())?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn parse_sync_mode(sync: &str, flush_every_ms: u64, flush_every_events: u64) -> PyResult<SyncMode> {
    match sync {
        "always" => Ok(SyncMode::Always),
        "batch" => Ok(SyncMode::Batch {
            flush_every_events,
            flush_every_ms,
        }),
        other => Err(PyValueError::new_err(format!(
            "unsupported sync mode {other:?}; expected 'always' or 'batch'"
        ))),
    }
}

fn py_err(err: ForgeError) -> PyErr {
    match err {
        ForgeError::InvalidDuration(_) => PyValueError::new_err(err.to_string()),
        _ => PyRuntimeError::new_err(err.to_string()),
    }
}

fn job_to_py(py: Python<'_>, job: forge_core::Job) -> PyResult<PyObject> {
    let dict = PyDict::new_bound(py);
    dict.set_item("id", job.id)?;
    dict.set_item("queue", job.queue)?;
    dict.set_item("payload", job.payload)?;
    dict.set_item("priority", job.priority)?;
    dict.set_item("created_at", job.created_at.to_rfc3339())?;
    dict.set_item("scheduled_at", job.scheduled_at.to_rfc3339())?;
    dict.set_item("max_attempts", job.max_attempts)?;
    dict.set_item("attempt", job.attempt)?;
    dict.set_item("status", status_name(job.status))?;
    dict.set_item("last_error", job.last_error)?;
    dict.set_item("claimed_at", job.claimed_at.map(|dt| dt.to_rfc3339()))?;
    dict.set_item("completed_at", job.completed_at.map(|dt| dt.to_rfc3339()))?;
    Ok(dict.into_py(py))
}

fn status_name(status: JobStatus) -> &'static str {
    match status {
        JobStatus::Queued => "queued",
        JobStatus::Claimed => "claimed",
        JobStatus::Succeeded => "succeeded",
        JobStatus::Failed => "failed",
        JobStatus::Retrying => "retrying",
        JobStatus::Dead => "dead",
    }
}

fn json_to_py(py: Python<'_>, value: serde_json::Value) -> PyResult<PyObject> {
    let json = py.import_bound("json")?;
    let encoded =
        serde_json::to_string(&value).map_err(|err| PyRuntimeError::new_err(err.to_string()))?;
    Ok(json.call_method1("loads", (encoded,))?.unbind().into())
}
