use std::{
    collections::BinaryHeap,
    fs::{self, File, OpenOptions},
    io::{self, Error, Read, Seek, Write},
    os::unix::fs::MetadataExt,
    sync::Arc,
};

use tokio::task::JoinSet;

use crate::{bucket, Config, BLOCK_SIZE};

struct Block {
    file_idx: usize,
    block: [u8; BLOCK_SIZE],
}

impl Ord for Block {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.block.cmp(&other.block)
    }
}

impl Eq for Block {}

impl PartialOrd for Block {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.block.partial_cmp(&other.block)
    }
}

impl PartialEq for Block {
    fn eq(&self, other: &Self) -> bool {
        self.block.eq(&other.block)
    }
}

pub async fn sort(cfg: crate::Config) -> io::Result<()> {
    println!("Split");
    let mut files = split(&cfg).await?;
    println!("Sort");
    let mut file = OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(&cfg.file)?;

    let mut heap = BinaryHeap::new();
    let mut buf = [0 as u8; crate::BLOCK_SIZE as usize];
    println!("Sort loop");

    // Populate the heap.
    for (i, mut f) in files.iter().enumerate() {
        f.seek(io::SeekFrom::Start(0))?;
        let n = f.read(&mut buf)?;
        let b = Block {
            file_idx: i,
            block: buf.clone(),
        };
        heap.push(b);
    }
    loop {
        // THE PROBLEM HERE IS THAT I'M POPPING 1 BLOCK, BUT READING N BLOCKS IN THE NEXT ITERATION.
        // I gotta fill it back with a block coming from the same file from which the popped block came.

        // let mut keep = vec![true; files.len()];
        let last_popped_file_idx: usize;
        match heap.pop() {
            Some(b) => {
                last_popped_file_idx = b.file_idx;
                file.write(&b.block)?;
            } //write b to file,
            None => break, // The heap is empty - we're done.
        }
        let n = files[last_popped_file_idx].read(&mut buf)?;
        if n == 0 {
            files.remove(last_popped_file_idx);
        }
        if n != buf.len() {
            return Result::Err(Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("Expected to read {}B, but read only {}B", buf.len(), n),
            ));
        }
        let b = Block {
            file_idx: last_popped_file_idx,
            block: buf.clone(),
        };
        heap.push(b);
        // if !files.is_empty() {
        //     let mut iter = keep.iter();
        //     files.retain(|_| *iter.next().unwrap());
        //     if files.is_empty() {
        //         break;
        //     }
        // }
    }
    Ok(())
}

async fn split(cfg: &Config) -> io::Result<Vec<File>> {
    let file = File::open(&cfg.file).map_err(|e| {
        Error::new(
            io::ErrorKind::Other,
            format!("Error opening source file {}: {}", cfg.file, e),
        )
    })?;
    let meta = file.metadata().map_err(|e| {
        Error::new(
            io::ErrorKind::Other,
            format!("Error getting source file {} metadata: {}", cfg.file, e),
        )
    })?;
    println!("size: {}", meta.size());
    let no_intermediate_files = meta.size() / cfg.int_file_size;
    println!("Intermediate files {no_intermediate_files}");
    // More workers means more allocations, which can cause memory swaps since the disk is the
    // bottleneck. If a thread is spawned for every core (10 on my mac m1 pro), the split phase
    // takes >400% longer (25s vs 2m 40s).
    let b = bucket::Bucket::new(cfg.split_concurrency);
    let b = Arc::new(b);
    let mut set = JoinSet::new();
    let int_file_dir = &cfg.int_file_dir.clone();
    fs::create_dir_all(int_file_dir).map_err(|e| {
        Error::new(
            io::ErrorKind::Other,
            format!("Error creating int. file dir {}: {}", cfg.int_file_dir, e),
        )
    })?;
    let int_filenames =
        (0..no_intermediate_files).map(|i| format!("{}/{}.txt", int_file_dir, i.to_string()));
    for (i, filename) in int_filenames.into_iter().enumerate() {
        let b = b.clone();
        let name = cfg.file.to_owned().clone();
        let int_file_size = cfg.int_file_size;
        set.spawn_blocking(move || {
            let mut f = OpenOptions::new()
                .create(true)
                .write(true)
                .read(true)
                .truncate(true)
                .open(&filename)
                .map_err(|e| {
                    Error::new(
                        io::ErrorKind::Other,
                        format!("Error opening int. file {}: {}", filename, e),
                    )
                })?;
            let mut file = File::open(name.clone()).map_err(|e| {
                Error::new(
                    io::ErrorKind::Other,
                    format!("Error opening source file {}: {}", name, e),
                )
            })?;

            let o = i as u64 * int_file_size;
            let offset = io::SeekFrom::Start(o);
            file.seek(offset)?;

            b.take();
            let mut buf = vec![0 as u8; int_file_size as usize];
            // TODO: What if bytes_read < int_file_size? E.g. if the source file doesn't align with
            // int_file_size.
            let bytes_read = file.read(buf.as_mut()).map_err(|e| {
                Error::new(
                    io::ErrorKind::Other,
                    format!("Error reading int. file {}: {}", name, e),
                )
            })?;
            let blocks_per_file = buf.len() / crate::BLOCK_SIZE;
            let mut blocks = Vec::with_capacity(blocks_per_file);
            for i in 0..blocks_per_file {
                // TODO: is buf.take(crate::BLOCK_SIZE) better?
                let offset = i * crate::BLOCK_SIZE;
                blocks.push(&buf[offset..offset + crate::BLOCK_SIZE]);
            }
            blocks.sort_unstable();

            // TODO: check written bytes match the expected val.
            match f.write(&blocks.concat()) {
                Ok(_) => {
                    b.put();
                    Ok(f)
                }
                Err(e) => Err(e),
            }
        });
    }
    let mut files = Vec::with_capacity(no_intermediate_files as usize);
    while let Some(res) = set.join_next().await {
        let f = res??;
        files.push(f);
    }
    Ok(files)
}
