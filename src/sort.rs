use log::{debug, info};
use std::{
    collections::{BinaryHeap, HashMap},
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
        other.block.cmp(&self.block)
    }
}

impl Eq for Block {}

impl PartialOrd for Block {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        other.block.partial_cmp(&self.block)
    }
}

impl PartialEq for Block {
    fn eq(&self, other: &Self) -> bool {
        self.block.eq(&other.block)
    }
}

pub async fn sort(cfg: crate::Config) -> io::Result<()> {
    let mut files = split(&cfg).await?;
    let mut file = OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(&cfg.file)?;

    let mut heap = BinaryHeap::new();
    let mut buf = [0 as u8; crate::BLOCK_SIZE as usize];

    // Populate the heap.
    info!("Populating the heap");
    for (i, mut f) in &files {
        f.rewind()?;
        let _n = f.read(&mut buf)?;
        let b = Block {
            file_idx: *i,
            block: buf.clone(),
        };
        heap.push(b);
        info!("Added from file {}", i);
    }
    while let Some(b) = heap.pop() {
        let last_popped_file_idx: usize;

        last_popped_file_idx = b.file_idx;
        let n = file.write(&b.block)?;
        debug!("Wrote {}B from file {}", n, b.file_idx);

        let f: &mut File;
        match files.get_mut(&last_popped_file_idx) {
            Some(file) => f = file,
            None => {
                files.remove(&last_popped_file_idx);
                continue;
            }
        }
        let n = f.read(&mut buf)?;
        if n == 0 {
            files.remove(&last_popped_file_idx);
            continue;
        }
        debug!("Read {n}B");
        let b = Block {
            file_idx: last_popped_file_idx,
            block: buf.clone(),
        };
        heap.push(b);
    }
    Ok(())
}

async fn split(cfg: &Config) -> io::Result<HashMap<usize, File>> {
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
    info!("Source file size: {}", meta.size());
    if meta.size() as usize % BLOCK_SIZE != 0 {
        return Err(Error::new(
            io::ErrorKind::Other,
            format!("Source file ({}B) is not page-aligned", meta.size()),
        ));
    }
    let no_intermediate_files = meta.size() / cfg.int_file_size as u64;
    info!("Intermediate files {no_intermediate_files}");
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
        debug!("Writing int. file {i}");
        let b = b.clone();
        let name = cfg.file.to_owned().clone();
        let int_file_size = cfg.int_file_size;
        set.spawn_blocking(move || {
            debug!("Opening int. file {i}");
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
            debug!("Opening source file to read file {i}'s contents");
            let mut file = File::open(name.clone()).map_err(|e| {
                Error::new(
                    io::ErrorKind::Other,
                    format!("Error opening source file {}: {}", name, e),
                )
            })?;

            let o = i * int_file_size;
            info!("Reading source file at offset {}", o);
            file.seek(io::SeekFrom::Start(o as u64))?;

            b.take();
            let mut buf = vec![0 as u8; int_file_size];

            file.read(&mut buf)?;

            let blocks_per_file = buf.len() / crate::BLOCK_SIZE;
            let mut blocks = Vec::with_capacity(blocks_per_file);
            for i in 0..blocks_per_file {
                // TODO: is buf.take(crate::BLOCK_SIZE) better?
                let offset = i * crate::BLOCK_SIZE;
                blocks.push(&buf[offset..offset + crate::BLOCK_SIZE]);
            }

            debug!("Sorting file {i} contents");
            blocks.sort_unstable();

            // TODO: check written bytes match the expected val.
            debug!("Writing to file {i}");
            let contents = blocks.concat();
            f.write_all(&contents)?;
            match f.flush() {
                Ok(_) => {
                    b.put();
                    Ok((i, f))
                }
                Err(e) => Err(e),
            }
        });
    }
    let mut files = HashMap::with_capacity(no_intermediate_files as usize);
    debug!("Waiting for writer threads");

    while let Some(res) = set.join_next().await {
        let (i, f) = res??;
        files.insert(i, f);
        debug!("Joined writer thread")
    }
    Ok(files)
}
