use std::{
    fs::File,
    io::{self, Read, Seek, Write},
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
    let _res = split(&cfg).await?;
    Ok(())
}

async fn split(cfg: &Config) -> io::Result<Vec<File>> {
    let file = File::open(&cfg.file)?;
    let meta = file.metadata()?;
    println!("size: {}", meta.size());
    let num_cores = std::thread::available_parallelism().unwrap().get();
    let no_intermediate_files = meta.size() / cfg.int_file_size;
    println!("Intermediate files {no_intermediate_files}");
    let b = bucket::Bucket::new(num_cores as i32);
    let b = Arc::new(b);
    let mut set = JoinSet::new();
    for i in 0..no_intermediate_files {
        let b = b.clone();
        let name = cfg.file.to_owned().clone();
        let int_file_dir = cfg.int_file_dir.clone();
        let int_file_size = cfg.int_file_size;
        set.spawn_blocking(move || {
            fs::create_dir_all(&int_file_dir)?;
            let mut f = File::create(format!("{}/{}.txt", int_file_dir, i.to_string()))?;
            let mut file = File::open(name.clone())?;

            let o = i as u64 * int_file_size;
            let offset = io::SeekFrom::Start(o);
            file.seek(offset)?;

            let mut buf = vec![0 as u8; int_file_size as usize];
            b.take();
            // TODO: What if bytes_read < int_file_size? E.g. if the source file doesn't align with
            // int_file_size.
            let bytes_read = file.read(buf.as_mut())?;
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
