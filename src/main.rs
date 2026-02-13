
mod store;
mod resp;
mod db;
mod queue;
mod server;

use server::run_server;

#[tokio::main]
async fn main() {
    if let Err(e) = run_server("0.0.0.0:6379").await {
        eprintln!("Server error: {}", e);
    }
}
