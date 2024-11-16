use clap::Args;

#[derive(Args)]
pub struct GenArgs {
    /// The filepath.
    #[arg(short, long)]
    pub file: String,
    /// The size of the file to generate.
    #[arg(short, long)]
    pub size: usize,
    /// The maxium amount of memory to be used by this program.
    #[arg(short, long, default_value_t = crate::ONE_GIB * 2)] // 2GiB
    pub max_mem: usize,
}

// async fn generate(cfg: &Gen) -> io::Result<()> {
//     let file = File::create(cfg.file.clone()).await?;
//     generate::generate_data(file, s, cfg.max_mem).await
// match res {
//     Err(e) => {
//         eprintln!("{e}");
//         process::exit(1);
//     }
//     Ok(_) => {
//         println!("File generated at {}", cfg.file);
//         return Ok(());
//     }
// }
// }
