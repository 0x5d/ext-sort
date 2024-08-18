use std::{
    fs::File,
    io::{self, Read, Seek, Write},
    os::unix::fs::MetadataExt,
    sync::Arc,
};

use tokio::task::JoinSet;

use crate::{bucket, Config};

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
        let int_file_size = cfg.int_file_size;
        set.spawn_blocking(move || {
            let mut f = File::create(format!("int-{}.txt", i.to_string()))?;
            let mut file = File::open(name.clone())?;

            let o = i as u64 * int_file_size;
            let offset = io::SeekFrom::Start(o);
            file.seek(offset)?;

            let mut buf = vec![0 as u8; int_file_size as usize];
            b.take();
            // TODO: What if bytes_read < int_file_size? E.g. if the source file doesn't align with
            // int_file_size.
            let bytes_read = file.read(buf.as_mut())?;

            // TODO: check written bytes match the expected val.
            match f.write(&buf[..bytes_read]) {
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
