use std::{env, fs, io, path::Path};

use crate::BLOCK_SIZE;

pub async fn check_int_files(cfg: crate::Config) -> io::Result<()> {
    let dir_path = Path::new(&cfg.int_file_dir);
    let wd = env::current_dir()?;
    println!("Listing files in {dir_path:?} - wd {wd:?}");
    let dir = fs::read_dir(dir_path)?;
    let placeholder: Vec<u8> = vec![];
    for f in dir {
        let f = f?;
        let path = f.path();
        println!("Checking file {path:?}");
        let buf = fs::read(path)?;
        let blocks = buf.chunks(BLOCK_SIZE);
        let mut last: &[u8] = placeholder.as_ref();
        for (i, block) in blocks.enumerate() {
            if block < last {
                panic!("Block {i} is less than the previous one");
            }
            last = block;
        }
    }
    Ok(())
}
