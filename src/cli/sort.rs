use clap::Args;

#[derive(Args)]
pub struct SortArgs {
    /// The path to the output file.
    #[arg(short, long)]
    pub file: String,
    /// The maxium intermediate file size.
    #[arg(short, long, default_value_t = crate::ONE_GIB * 2)] // 2GiB
    pub int_file_size: usize,
    /// The directory to create intermediate files.
    #[arg(short, long, default_value_t = String::from("./int"))]
    pub int_file_dir: String,
    /// The concurrency level (number of writer threads) during the split phase.
    #[arg(short, long, default_value_t = 2)]
    pub split_concurrency: i32,
}
