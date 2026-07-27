//! Connect to a SHIP endpoint and print the ABI it announces.
//!
//! Usage: cargo run -p ship --example dump_abi -- ws://host:port

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let url = std::env::args().nth(1).expect("usage: dump_abi <ws-url>");
    let client = ship::ShipClient::connect(&url).await?;
    println!("{}", serde_json::to_string_pretty(&client.abi)?);
    Ok(())
}
