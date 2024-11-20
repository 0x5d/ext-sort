use rand::distributions::{Alphanumeric, DistString};
use rand::rngs::SmallRng;
use rand::SeedableRng;
use std::cmp::min;
use std::fs::File;
use std::io::{self, Error, ErrorKind, Write};
use std::sync::mpsc::Sender;
use std::sync::{mpsc, Arc};

use mpsc::Receiver;
use tokio::task::JoinSet;

use crate::bucket::Bucket;

/// The value sent by workers to the writer when they have finished processing data.
const POISON_PILL: &str = "shutdown now";

/// Write size_bytes random data into file, using at most max_mem (RAM).
pub async fn generate_data(filepath: &str, size_bytes: usize, max_mem: usize) -> io::Result<()> {
    if (max_mem as usize) < crate::BLOCK_SIZE {
        return io::Result::Err(Error::new(
            ErrorKind::Other,
            format!(
                "Max allowed memory must be larger than {}B",
                crate::BLOCK_SIZE
            ),
        ));
    }
    let file = File::create(filepath)?;
    let num_cores = std::thread::available_parallelism()?.get();
    let mem_per_core = max_mem / num_cores;
    let b = Arc::new(Bucket::new(num_cores as i32));
    let writer_bucket = b.clone();
    let mut set = JoinSet::new();
    let (tx, rx) = mpsc::channel();

    let writer_handle = tokio::spawn(writer(file, writer_bucket, rx));

    let mut remaining = size_bytes;
    while remaining > 0 {
        let len = min(remaining, mem_per_core);
        remaining -= len;
        let tx = tx.clone();
        b.take();
        set.spawn_blocking(move || generate(tx, len));
    }

    while let Some(res) = set.join_next().await {
        let _ = res??;
    }
    tx.send(String::from(POISON_PILL))
        .map_err(|e| io::Error::new(ErrorKind::Other, e))?;

    writer_handle.await?
}

async fn writer(mut file: File, b: Arc<Bucket>, rx: Receiver<String>) -> io::Result<()> {
    loop {
        let s = rx.recv().map_err(|e| {
            Error::new(
                ErrorKind::Other,
                format!("Could not receive from channel: {e}"),
            )
        })?;
        if s == POISON_PILL {
            return Ok(());
        }
        b.put();
        file.write_all(s.as_bytes())?;
    }
}

fn generate(tx: Sender<String>, len: usize) -> io::Result<()> {
    let rand_str = Alphanumeric.sample_string(&mut SmallRng::from_entropy(), len);
    tx.send(rand_str)
        .map_err(|e| Error::new(ErrorKind::Other, format!("Could not send to channel: {e}")))
}
