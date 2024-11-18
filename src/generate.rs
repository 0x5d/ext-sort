use rand::distributions::{Alphanumeric, DistString};
use rand::SeedableRng;
use core::str;
use std::alloc::{alloc, dealloc, Layout};
use std::fs::{File, OpenOptions};
use std::io::{self, Error};
use std::os::unix::fs::FileExt;
use std::sync::Arc;

use tokio::task::JoinSet;

use crate::{bucket, BLOCK_SIZE, ONE_GIB};

/// Write size_bytes random data into file, using at most max_mem (RAM).
pub async fn generate_data(filepath: &str, size_bytes: usize, max_mem: usize) -> io::Result<()> {
    if (max_mem as usize) < crate::BLOCK_SIZE {
        return io::Result::Err(Error::new(
            io::ErrorKind::Other,
            format!(
                "Max allowed memory must be larger than {}B",
                crate::BLOCK_SIZE
            ),
        ));
    }
    let file = File::create(filepath)?;
    let num_cores = std::thread::available_parallelism()?.get();
    let mem_per_core = max_mem / num_cores;
    let b = Arc::new(bucket::Bucket::new(num_cores as i32));
    let mut set: JoinSet<io::Result<()>> = JoinSet::new();

    let mut remaining = size_bytes;
    let mut offset: usize = 0;
    while remaining > 0 {
        let len = if remaining < mem_per_core {
            remaining
        } else {
            mem_per_core
        };

        b.take();
        let writer_bucket = b.clone();
        let filepath = filepath.to_owned().clone();
        set.spawn_blocking(move || {
            let f = OpenOptions::new().write(true).open(filepath)?;
            let layout = Layout::new::<[u8; 134_217_728]>();
            unsafe {
                let ptr = alloc(layout);
        
                f.write_all_at(std::slice::from_raw_parts(ptr, 134_217_728), offset as u64)?;
        
                dealloc(ptr, layout);
            }
            writer_bucket.put();
            Ok(())
        });
        remaining -= len;
        offset += len;
    }
    while let Some(res) = set.join_next().await {
        let _ = res??;
    }
    file.set_len(size_bytes as u64)
}
