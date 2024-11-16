use clap::Parser;
use log::LevelFilter;
use log4rs::{
    append::console::ConsoleAppender,
    config::{Appender, Root},
    encode::json::JsonEncoder,
};
use std::io::{self, Error};
use tokio::fs::File;

mod bucket;
mod check;
mod cli;
mod generate;
mod sort;

const BLOCK_SIZE: usize = 4096;
const ONE_GIB: usize = 1073741824;

#[tokio::main]
async fn main() -> io::Result<()> {
    let cli = cli::Cli::parse();

    let stdout: ConsoleAppender = ConsoleAppender::builder()
        .encoder(Box::new(JsonEncoder::new()))
        .build();
    let log_config = log4rs::config::Config::builder()
        .appender(Appender::builder().build("stdout", Box::new(stdout)))
        .build(Root::builder().appender("stdout").build(LevelFilter::Error))
        .unwrap();
    log4rs::init_config(log_config).unwrap();

    match &cli.command {
        cli::Commands::Gen(args) => {
            if (args.max_mem as usize) < BLOCK_SIZE {
                return io::Result::Err(Error::new(
                    io::ErrorKind::Other,
                    format!("Max allowed memory must be larger than {BLOCK_SIZE}B"),
                ));
            }
            let file = File::create(args.file.clone()).await?;
            generate::generate_data(file, args.size, args.max_mem).await
        }
        cli::Commands::Sort(args) => {
            sort::sort(
                &args.file,
                &args.int_file_dir,
                args.int_file_size,
                args.split_concurrency,
            )
            .await
        }
        cli::Commands::Check(args) => check::check(&args.int_file_dir, &args.file),
    }
}
