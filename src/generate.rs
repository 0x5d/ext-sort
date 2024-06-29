use rand::distributions::{Alphanumeric, DistString};
use std::io::{self, ErrorKind};
use std::sync::mpsc;

use mpsc::Receiver;
use tokio::io::AsyncWriteExt;
use tokio::{fs::File, task::JoinSet};

pub async fn generate_data(file: File, size_bytes: u64) -> io::Result<()> {
    let (tx, rx) = mpsc::channel();
    let writer_handle = writer(file, rx);
    let mut set = JoinSet::new();
    let mut remaining = size_bytes;

    while remaining > 0 {
        let to_write = if remaining < crate::BLOCK_SIZE * 4 {
            remaining
        } else {
            crate::BLOCK_SIZE * 4
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

fn generate(len: u64) -> io::Result<String> {
    let s = Alphanumeric.sample_string(&mut rand::thread_rng(), len as usize);
    Ok(s)
}
