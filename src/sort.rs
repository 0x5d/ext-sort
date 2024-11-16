use log::{debug, info};
use std::{
    collections::{BinaryHeap, HashMap},
    fs::{self, File, OpenOptions},
    io::{self, BufReader, BufWriter, Error, Read, Seek, Write},
    os::unix::fs::MetadataExt,
    sync::{mpsc::{self, Receiver}, Arc},
};

use tokio::task::JoinSet;

use crate::{
    bucket::{self, Bucket},
    Config, BLOCK_SIZE,
};

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
    let target_file = OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(&cfg.file)?;

    let target_file = BufWriter::with_capacity(BLOCK_SIZE * 512, target_file);

    let (tx, rx) = mpsc::channel();
    let writer_handle = tokio::spawn( writer_worker(target_file, rx));
    
    let mut heap = BinaryHeap::new();
    let mut buf = [0 as u8; crate::BLOCK_SIZE];

    // Populate the heap.
    info!("Populating the heap");
    for (i, f) in files.iter_mut() {
        let _n = f.read(&mut buf)?;
        heap.push(Block {
            file_idx: *i,
            block: buf.clone(),
        });
        info!("Added from file {}", i);
    }
    while let Some(b) = heap.pop() {
        let last_popped_file_idx: usize;

        last_popped_file_idx = b.file_idx;

        let _ = tx.send(b.block);

        let f: &mut BufReader<File>;
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
    
    let _ = tx.send(POISON_PILL.to_owned());
    writer_handle.await?;
    Ok(())
}

async fn split(cfg: &Config) -> io::Result<HashMap<usize, BufReader<File>>> {
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
    for (i, int_filename) in int_filenames.into_iter().enumerate() {
        debug!("Writing int. file {i}");
        let b = b.clone();
        let source_filename = cfg.file.to_owned().clone();
        let int_file_size = cfg.int_file_size;
        set.spawn_blocking(move || write_intermediate_file(i, source_filename, int_filename, int_file_size, b));
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

fn write_intermediate_file(
    i: usize,
    source_filename: String,
    int_filename: String,
    int_file_size: usize,
    b: Arc<Bucket>,
) -> io::Result<(usize, BufReader<File>)> {
    debug!("Opening int. file {i}");
    let mut f = OpenOptions::new()
        .create(true)
        .write(true)
        .read(true)
        .truncate(true)
        .open(&int_filename)
        .map_err(|e| {
            Error::new(
                io::ErrorKind::Other,
                format!("Error opening int. file {}: {}", int_filename, e),
            )
        })?;
    debug!("Opening source file to read file {i}'s contents");
    let mut file = File::open(source_filename.clone()).map_err(|e| {
        Error::new(
            io::ErrorKind::Other,
            format!("Error opening source file {}: {}", source_filename, e),
        )
    })?;

    let offset = i * int_file_size;
    info!("Reading source file at offset {}", offset);
    file.seek(io::SeekFrom::Start(offset as u64))?;

    b.take();
    let mut buf = vec![0 as u8; int_file_size];

    file.read(&mut buf)?;

    let blocks_per_file = buf.len() / crate::BLOCK_SIZE;
    let mut blocks = Vec::with_capacity(blocks_per_file);
    for i in 0..blocks_per_file {
        let offset = i * crate::BLOCK_SIZE;
        blocks.push(&buf[offset..offset + crate::BLOCK_SIZE]);
    }

    debug!("Sorting file {i} contents");
    blocks.sort_unstable();

    // TODO: check written bytes match the expected val.
    debug!("Writing to file {i}");
    let contents = blocks.concat();
    // f.write_all(&contents)?;

    match f.write_all(&contents) {
        Ok(_) => {
            b.put();
            f.rewind()?;
            Ok((i, BufReader::with_capacity(BLOCK_SIZE * 256, f)))
        }
        Err(e) => Err(e),
    }
}


/// The value sent by workers to the writer when they have finished processing data.
const POISON_PILL: [u8; BLOCK_SIZE] = [0u8; BLOCK_SIZE];

async fn writer_worker(mut file: BufWriter<File>, rx: Receiver<[u8; BLOCK_SIZE]>) {
    loop {
        // TODO handle error.
        match rx.recv() {
            Ok(s) => {
                if s == POISON_PILL {
                    return;
                }
                file.write_all(&s).unwrap();
            }
            Err(e) => {
                println!("{e}");
                return;
            }
        }
    }
}