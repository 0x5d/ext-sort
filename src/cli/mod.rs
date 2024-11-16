use clap::{command, Parser, Subcommand};

mod check;
mod gen;
mod sort;

#[derive(Parser)]
#[command(version, about, long_about = None)]
#[command(propagate_version = true)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    Gen(gen::GenArgs),
    Sort(sort::SortArgs),
    Check(check::CheckArgs),
}
