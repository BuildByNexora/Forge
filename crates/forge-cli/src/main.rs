use std::path::PathBuf;

use clap::{Parser, Subcommand};
use forge_core::Queue;

#[derive(Parser)]
#[command(name = "forge", about = "embedded persistent job queue")]
struct Cli {
    #[arg(long, global = true, default_value = ".forge")]
    data_dir: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// List all jobs in the queue
    Queue {
        #[command(subcommand)]
        command: QueueCommand,
    },
    /// Inspect a specific job
    Job {
        #[command(subcommand)]
        command: JobCommand,
    },
    /// Dead letter queue operations
    Dead {
        #[command(subcommand)]
        command: DeadCommand,
    },
    /// Diagnostic report
    Doctor,
    /// Compact the AOF log
    Compact,
}

#[derive(Subcommand)]
enum QueueCommand {
    /// List all jobs
    List,
}

#[derive(Subcommand)]
enum JobCommand {
    /// Show job status
    Status { job_id: String },
    /// Show job history
    History { job_id: String },
}

#[derive(Subcommand)]
enum DeadCommand {
    /// List dead letter queue
    List,
    /// Retry a dead job
    Retry { job_id: String },
}

fn main() {
    let cli = Cli::parse();
    if let Err(err) = run(cli) {
        eprintln!("forge: {err}");
        std::process::exit(1);
    }
}

fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    match cli.command {
        Command::Queue { command } => match command {
            QueueCommand::List => {
                let queue = Queue::open(&cli.data_dir)?;
                let jobs = queue.list()?;
                if jobs.is_empty() {
                    println!("[]");
                    return Ok(());
                }
                println!("{}", serde_json::to_string_pretty(&jobs)?);
            }
        },
        Command::Job { command } => match command {
            JobCommand::Status { job_id } => {
                let queue = Queue::open(&cli.data_dir)?;
                match queue.status(&job_id)? {
                    Some(job) => println!("{}", serde_json::to_string_pretty(&job)?),
                    None => eprintln!("job not found: {job_id}"),
                }
            }
            JobCommand::History { job_id } => {
                let queue = Queue::open(&cli.data_dir)?;
                let history = queue.history(&job_id)?;
                if history.is_empty() {
                    eprintln!("no history for job: {job_id}");
                    return Ok(());
                }
                println!("{}", serde_json::to_string_pretty(&history)?);
            }
        },
        Command::Dead { command } => match command {
            DeadCommand::List => {
                let queue = Queue::open(&cli.data_dir)?;
                let dead = queue.dead_list()?;
                if dead.is_empty() {
                    println!("[]");
                    return Ok(());
                }
                println!("{}", serde_json::to_string_pretty(&dead)?);
            }
            DeadCommand::Retry { job_id } => {
                let queue = Queue::open(&cli.data_dir)?;
                queue.dead_retry(&job_id)?;
                println!("retried {job_id}");
            }
        },
        Command::Doctor => {
            let queue = Queue::open(&cli.data_dir)?;
            println!("{}", serde_json::to_string_pretty(&queue.doctor()?)?);
        }
        Command::Compact => {
            let queue = Queue::open(&cli.data_dir)?;
            queue.compact()?;
            println!("compacted");
        }
    }
    Ok(())
}
