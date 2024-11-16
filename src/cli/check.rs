use clap::Args;

#[derive(Args)]
pub struct CheckArgs {
    /// The path to the output file.
    #[arg(short, long)]
    pub file: String,
    /// The path to the intermediate intermediate files directory.
    #[arg(short, long, default_value_t = String::from("./int"))]
    pub int_file_dir: String,
}
