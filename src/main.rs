use std::{
    fs::File,
    io::{self, Write},
    process,
};

use clap::{command, Parser};
use io::{Error, ErrorKind, Result};
use rand::distributions::{Alphanumeric, DistString};

const BLOCK_SIZE: usize = 4096;

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
/// Generate & sort big files.
struct Cli {
    /// If set, will generate a file of the given size at the given path.
    #[arg(short, long, default_value_t = false)]
    generate: bool,
    /// If set, will sort the file at the given path.
    #[arg(long, default_value_t = false)]
    sort: bool,
    /// The filepath.
    #[arg(short, long)]
    file: String,
    /// The size of the file to generate.
    #[arg(short, long)]
    size: Option<usize>,
    /// The size of the file to generate.
    #[arg(short, long, default_value_t = 536870912)]
    max_mem: usize,
}

fn main() {
    let cli = Cli::parse();
    if cli.max_mem < BLOCK_SIZE {
        eprintln!("Max allowed memory must be larger than {BLOCK_SIZE}B");
        process::exit(1);
    }
    if cli.generate {
        let res = match cli.size {
            None => Result::Err(Error::new(
                ErrorKind::InvalidInput,
                "If --generate is chosen, --size must be set.",
            )),
            Some(s) => generate_data(&cli.file, s),
        };
        match res {
            Err(e) => {
                eprintln!("{e}");
                process::exit(1);
            }
            Ok(_) => println!("File generated at {}", cli.file),
        }
    }
}

// TODO: Make concurrent.
fn generate_data(file: &str, size_bytes: usize) -> Result<()> {
    let mut size = size_bytes;
    let rem = size_bytes % BLOCK_SIZE;
    if rem > 0 {
        size -= rem;
        println!("Size ({size_bytes}) is not page-alligned. Truncating to {size}.");
    }
    let mut file = File::create(file)?;

    let mut thread_rng = rand::thread_rng();

    let mut remaining = size;
    while remaining > 0 {
        let to_write = if remaining < BLOCK_SIZE * 4 {
            remaining
        } else {
            BLOCK_SIZE * 4
        };
        // For some reason, Alphanumeric.sample_string hangs when generating large strings (~1GiB);
        let string = Alphanumeric.sample_string(&mut thread_rng, to_write);
        file.write_all(string.as_bytes())?;

        remaining -= to_write;
    }
    file.flush()?;
    Ok(())
}
