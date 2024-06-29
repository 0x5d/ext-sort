use clap::{command, Parser};
use std::{
    io::{self, ErrorKind},
    process,
};
use tokio::fs::File;

mod generate;

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

#[tokio::main]
async fn main() -> io::Result<()> {
    let cli = Cli::parse();
    if cli.max_mem < BLOCK_SIZE {
        eprintln!("Max allowed memory must be larger than {BLOCK_SIZE}B");
        process::exit(1);
    }
    if cli.generate {
        let res = match cli.size {
            None => Result::Err(io::Error::new(
                ErrorKind::InvalidInput,
                "If --generate is chosen, --size must be set.",
            )),
            Some(s) => {
                let file = File::create(cli.file.clone()).await?;
                generate::generate_data(file, s).await
            }
        };
        match res {
            Err(e) => {
                eprintln!("{e}");
                process::exit(1);
            }
            Ok(_) => println!("File generated at {}", cli.file),
        }
    }
    Ok(())
}
