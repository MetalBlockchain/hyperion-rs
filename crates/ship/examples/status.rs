//! Connect to a SHIP endpoint and print its get_status result.
//!
//! Usage: cargo run -p ship --example status -- ws://host:port

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let url = std::env::args().nth(1).expect("usage: status <ws-url>");
    let mut client = ship::ShipClient::connect(&url).await?;
    let status = client.get_status().await?;
    println!("{status:#?}");
    Ok(())
}
