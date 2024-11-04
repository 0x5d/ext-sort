use clap::{command, Parser};
use log::LevelFilter;
use log4rs::{
    append::console::ConsoleAppender,
    config::{Appender, Root},
    encode::json::JsonEncoder,
};
use std::{
    io::{self, ErrorKind},
    process,
};
use tokio::fs::File;

mod bucket;
mod check;
mod generate;
mod sort;

const BLOCK_SIZE: usize = 4096;
const ONE_GIB: usize = 1073741824;

/// Generate & sort big files.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Config {
    /// If set, will generate a file of the given size at the given path.
    #[arg(short, long, default_value_t = false)]
    generate: bool,
    /// If set, will sort the file at the given path.
    #[arg(long, default_value_t = false)]
    sort: bool,
    /// If set, will check the intermediate files at the given path.
    #[arg(long, default_value_t = false)]
    check: bool,
    /// The filepath.
    #[arg(short, long)]
    file: String,
    /// The size of the file to generate.
    #[arg(short, long)]
    size: Option<usize>,
    /// The maxium amount of memory to be used by this program.
    #[arg(short, long, default_value_t = ONE_GIB * 2)] // 2GiB
    max_mem: usize,
    /// The directory to create intermediate files.
    #[arg(short, long, default_value_t = String::from("/int"))] // 2GiB
    int_file_dir: String,
    /// The maxium intermediate file size.
    #[arg(short, long, default_value_t = ONE_GIB * 2)] // 2GiB
    int_file_size: usize,
    /// The concurrency level (number of writer threads) during the split phase.
    #[arg(short, long, default_value_t = 2)]
    split_concurrency: i32,
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let cfg = Config::parse();
    if (cfg.max_mem as usize) < BLOCK_SIZE {
        eprintln!("Max allowed memory must be larger than {BLOCK_SIZE}B");
        process::exit(1);
    }
    if cfg.generate {
        generate(&cfg).await?
    } else if cfg.sort {
        sort::sort(cfg).await?
    } else if cfg.check {
        check::check(&cfg)?;
    } else {
        eprintln!("One of --generate or --sort must be passed.");
        process::exit(1);
    }
    Ok(())
}

async fn generate(cfg: &Config) -> io::Result<()> {
    let res = match cfg.size {
        None => Result::Err(io::Error::new(
            ErrorKind::InvalidInput,
            "If --generate is chosen, --size must be set.",
        )),
        Some(s) => {
            let file = File::create(cfg.file.clone()).await?;
            generate::generate_data(file, s, cfg.max_mem).await
        }
    };
    match res {
        Err(e) => {
            eprintln!("{e}");
            process::exit(1);
        }
        Ok(_) => {
            println!("File generated at {}", cfg.file);
            return Ok(());
        }
    }
}