use std::{
    fs::{self, File},
    io::{self, BufReader, Read},
    path::{Path, PathBuf},
};

use log::{debug, error, info};

use crate::BLOCK_SIZE;

pub fn check(int_file_dir: &str, file: &str) -> io::Result<()> {
    if !int_file_dir.is_empty() {
        check_int_files(int_file_dir)?;
    } else {
        info!("Intermediate files dir wasn't provided. Skipping intermediate files checking.")
    }
    check_file(Path::new(file).to_path_buf())
}

fn check_int_files(int_file_dir: &str) -> io::Result<()> {
    let dir_path = Path::new(&int_file_dir);
    let dir = fs::read_dir(dir_path)?;
    for entry in dir {
        let entry = entry?;
        check_file(entry.path())?;
    }
    Ok(())
}

fn check_file(path: PathBuf) -> io::Result<()> {
    debug!("Checking file {:?}", path);
    let f = File::open(&path)?;
    let mut reader = BufReader::new(f);
    let mut last = [0; BLOCK_SIZE];
    let mut current = [0; BLOCK_SIZE];

    reader.read_exact(&mut last)?;
    let mut block = 0;

    while let Ok(()) = reader.read_exact(&mut current) {
        if current == last {
            error!("current == last");
        }
        if current < last {
            panic!("Block {block} is less than the previous one");
        }
        last = current;
        block += 1;
    }
    Ok(())
}
