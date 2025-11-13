use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use std::sync::Arc;
use tokio::sync::Semaphore;

#[tokio::main]
async fn main() {
    // List of directories to process (pretend this is 100+ dirs)
    let dirs = vec![
        "day01".to_string(),
        "day02".to_string(),
        "day03".to_string(),
        "day04".to_string(),
        "day05".to_string(),
        "day06".to_string(),
        // ... many more
    ];

    // Limit: never process more than 4 directories at once
    let sem = Arc::new(Semaphore::new(4));

    // Collect JoinHandles so we can wait for everything at the end
    let mut handles = vec![];

    for dir in dirs {
        let sem_clone = sem.clone();

        // Acquire one permit (wait here if 4 are already running)
        let permit = sem_clone.acquire_owned().await.unwrap();

        // Move directory into a blocking CPU task
        let handle = tokio::task::spawn_blocking(move || {
            println!("[DIR] Starting: {dir}");

            // IMPORTANT: This runs on its own OS thread,
            // not inside the async runtime.
            process_directory(&dir);

            println!("[DIR] Finished: {dir}");

            // Drop permit so another directory can start
            drop(permit);
        });

        handles.push(handle);
    }

    // Wait for all directory jobs to finish
    for h in handles {
        h.await.unwrap();
    }

    println!("All directories completed.");
}

fn process_directory(dir: &str) {
    // Each directory gets its own dedicated Rayon pool.
    // This prevents nested threadpool deadlock and oversubscription.
    let pool = ThreadPoolBuilder::new()
        .num_threads(8) // e.g., 8 cores per directory
        .build()
        .unwrap();

    // Pretend each directory contains 24 gzip files
    let fake_gzips: Vec<u32> = (0..24).collect();

    pool.install(|| {
        fake_gzips.par_iter().for_each(|gz| {
            // This is your CPU-bound gzip → xml → flatten → batch work.
            let file = format!("{}/{}.xml.gz", dir, gz);
            process_gzip(&file);

            // Spawn async uploads from inside the Rayon thread.
            tokio::task::spawn(async move {
                upload_to_s3_async(file).await;
            });
        });
    });
}

fn process_gzip(path: &str) {
    // Your actual XML parsing + flattening goes here
    println!("    [GZIP] Processing {}", path);
}

async fn upload_to_s3_async(name: String) {
    // Fake async S3 upload
    println!("        [UPLOAD] {}", name);
}