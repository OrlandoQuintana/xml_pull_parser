use serde::Deserialize;
use config::Config;

#[derive(Debug, Deserialize)]
struct EtlConfig {
    data_dirs: Vec<String>,
    num_threads: Option<usize>,
    minio_bucket: Option<String>,
}

fn load_config(path: &str) -> anyhow::Result<EtlConfig> {
    let cfg = Config::builder()
        .add_source(config::File::with_name(path))
        .build()?;
    Ok(cfg.try_deserialize()?)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg = load_config("etl_config.toml")?;
    println!("Dirs: {:?}", cfg.data_dirs);

    for dir in cfg.data_dirs {
        process_day(dir).await?;
    }

    Ok(())
}


//[dependencies]
//serde = { version = "1", features = ["derive"] }
//toml = "0.8"
//config = "0.14"

