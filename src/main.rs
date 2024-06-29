use std::{io, process};

use std::sync::mpsc;

use clap::{command, Parser};
use io::ErrorKind;
use rand::distributions::{Alphanumeric, DistString};

use mpsc::Receiver;
use tokio::io::AsyncWriteExt;
use tokio::{fs::File, task::JoinSet};

const BLOCK_SIZE: usize = 4096;

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
/// Generate & sort big files.
struct Cli {
    /// If set, will generate a file of the given size at the given path.
    #[arg(short, long, default_value_t = false)]
    generate: bool,
    /// If set, will sort the file at the given path.
    #[arg(long, default_value_t = false)]
    sort: bool,
    /// The filepath.
    #[arg(short, long)]
    file: String,
    /// The size of the file to generate.
    #[arg(short, long)]
    size: Option<usize>,
    /// The size of the file to generate.
    #[arg(short, long, default_value_t = 536870912)]
    max_mem: usize,
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let cli = Cli::parse();
    if cli.max_mem < BLOCK_SIZE {
        eprintln!("Max allowed memory must be larger than {BLOCK_SIZE}B");
        process::exit(1);
    }
    if cli.generate {
        let res = match cli.size {
            None => Result::Err(io::Error::new(
                ErrorKind::InvalidInput,
                "If --generate is chosen, --size must be set.",
            )),
            Some(s) => {
                let file = File::create(cli.file.clone()).await?;
                generate_data(file, s).await
            }
        };
        match res {
            Err(e) => {
                eprintln!("{e}");
                process::exit(1);
            }
            Ok(_) => println!("File generated at {}", cli.file),
        }
    }
    Ok(())
}

async fn generate_data(file: File, size_bytes: usize) -> io::Result<()> {
    let (tx, rx) = mpsc::channel();
    let writer_handle = writer(file, rx);
    let mut set = JoinSet::new();
    let mut remaining = size_bytes;

    while remaining > 0 {
        let to_write = if remaining < BLOCK_SIZE * 4 {
            remaining
        } else {
            BLOCK_SIZE * 4
        };

        let tx = tx.clone();
        set.spawn_blocking(move || {
            generate(to_write)
                .and_then(|s| tx.send(s).map_err(|e| io::Error::new(ErrorKind::Other, e)))
        });

        remaining -= to_write;
        // println!("remaining: {remaining}")
    }
    while let Some(res) = set.join_next().await {
        let _ = res?;
        // println!("Joined writer.")
    }
    drop(tx);
    // println!("Waiting for writer");
    writer_handle.await;
    Ok(())
}

async fn writer(mut file: File, rx: Receiver<String>) {
    loop {
        // TODO handle error.
        match rx.recv() {
            Ok(s) => file.write_all(s.as_bytes()).await.unwrap(),
            Err(e) => {
                println!("{e}");
                return;
            }
        }
    }
}

fn generate(len: usize) -> io::Result<String> {
    let s = Alphanumeric.sample_string(&mut rand::thread_rng(), len);
    Ok(s)
}
