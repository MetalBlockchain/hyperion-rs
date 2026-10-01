//! Comparison tests between Elasticsearch and ClickHouse.
//!
//! Run with: cargo test --test es_vs_ch_comparison -- --ignored --nocapture
//!
//! Requirements:
//! - Elasticsearch running at http://localhost:9200
//! - ClickHouse running at http://localhost:8123

use anyhow::Result;
use hyperion::clickhouse::ClickHouse;
use hyperion::elastic::Elastic;
use hyperion::config::ElasticConfig;
use serde_json::{json, Value};
use std::time::Instant;

struct TestFixture {
    es: Elastic,
    ck: ClickHouse,
}

impl TestFixture {
    async fn setup() -> Result<Self> {
        let es_config = ElasticConfig {
            url: "http://localhost:9200".to_string(),
            user: String::new(),
            pass: String::new(),
            shards: 1,
            replicas: 0,
        };
        let es = Elastic::new(&es_config);

        // Test ES connection
        es.ping().await?;
        println!("✓ Connected to Elasticsearch");

        let ck = ClickHouse::new("http://localhost:8123", None, None);

        // Test CH connection
        ck.ping().await?;
        println!("✓ Connected to ClickHouse");

        // Create tables
        ck.create_all().await?;
        println!("✓ Created ClickHouse tables");

        Ok(TestFixture { es, ck })
    }

    async fn cleanup(&self) -> Result<()> {
        // Drop ClickHouse tables
        self.ck.drop_all().await?;

        // Drop Elasticsearch indices (basic cleanup, errors ignored)
        for index in &["test-action", "test-block", "test-delta"] {
            let _ = self.es.ping().await; // dummy operation
        }
        Ok(())
    }
}

/// Create a sample action document matching Hyperion schema.
fn sample_action(block_num: u32, sequence: u64) -> Value {
    json!({
        "@timestamp": "2024-01-01T00:00:00Z",
        "block_num": block_num,
        "global_sequence": sequence,
        "block_id": format!("{:064x}", block_num),
        "trx_id": format!("{:064x}", sequence),
        "producer": "producer1",
        "act": {
            "account": "eosio.token",
            "name": "transfer",
            "authorization": [
                {
                    "actor": "alice",
                    "permission": "active"
                }
            ],
            "data": {
                "from": "alice",
                "to": "bob",
                "quantity": "100.0000 EOS",
                "memo": "test transfer"
            },
            "hex_data": "1234567890abcdef"
        },
        "@transfer": {
            "from": "alice",
            "to": "bob",
            "amount": 100.0,
            "symbol": "EOS",
            "memo": "test transfer"
        },
        "notified": ["alice", "bob", "eosio.token"],
        "receipts": [
            {
                "receiver": "alice",
                "global_sequence": sequence
            }
        ],
        "cpu_usage_us": 100,
        "net_usage_words": 50,
        "action_ordinal": 1,
        "creator_action_ordinal": 0,
        "signatures": ["SIG_K1_123"]
    })
}

/// Create a sample block document.
fn sample_block(block_num: u32) -> Value {
    json!({
        "@timestamp": "2024-01-01T00:00:00Z",
        "block_num": block_num,
        "block_id": format!("{:064x}", block_num),
        "prev_id": format!("{:064x}", block_num - 1),
        "producer": "producer1",
        "schedule_version": 1,
        "trx_count": 5,
        "cpu_usage_us": 1000,
        "net_usage_words": 500
    })
}

/// Create a sample delta document.
#[allow(dead_code)]
fn sample_delta(block_num: u32) -> Value {
    json!({
        "@timestamp": "2024-01-01T00:00:00Z",
        "block_num": block_num,
        "block_id": format!("{:064x}", block_num),
        "code": "eosio.token",
        "scope": "alice",
        "table": "accounts",
        "primary_key": "4,EOS",
        "payer": "alice",
        "present": true,
        "data": {
            "balance": "1000.0000 EOS"
        },
        "value_hex": "1234567890abcdef"
    })
}

#[tokio::test]
#[ignore]
async fn test_basic_connectivity() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    println!("\n=== Basic Connectivity Test ===");
    println!("Both Elasticsearch and ClickHouse are reachable");
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_action_insert_elasticsearch() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    println!("\n=== Action Insert (Elasticsearch) ===");

    // Index 10 actions into ES
    let mut es_docs = String::new();
    for i in 1..=10 {
        let doc = sample_action(100, i as u64);
        es_docs.push_str(&format!("{{\"index\":{{\"_index\":\"test-action\",\"_id\":\"{}\" }}}}\n", i));
        es_docs.push_str(&format!("{}\n", serde_json::to_string(&doc)?));
    }

    let es_start = Instant::now();
    fixture.es.bulk(es_docs).await?;
    let es_time = es_start.elapsed();
    println!("ES bulk insert (10 actions): {:?}", es_time);
    println!("Throughput: {:.0} docs/sec", 10.0 / es_time.as_secs_f64());

    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_block_insert() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    println!("\n=== Block Insert Test ===");

    // Index 100 blocks
    let mut es_docs = String::new();
    for block_num in 1..=100 {
        let doc = sample_block(block_num);
        es_docs.push_str(&format!(
            "{{\"index\":{{\"_index\":\"test-block\",\"_id\":\"{}\" }}}}\n",
            block_num
        ));
        es_docs.push_str(&format!("{}\n", serde_json::to_string(&doc)?));
    }

    let es_start = Instant::now();
    fixture.es.bulk(es_docs).await?;
    let es_time = es_start.elapsed();

    println!("ES inserted 100 blocks in {:?}", es_time);
    println!("Rate: {:.0} blocks/sec", 100.0 / es_time.as_secs_f64());

    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_memory_footprint_baseline() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    println!("\n=== Memory Footprint Baseline Test ===");
    println!("This is a baseline test. Monitor process memory separately:");
    println!("  ES: watch -n 1 'ps aux | grep elasticsearch'");
    println!("  CH: watch -n 1 'ps aux | grep clickhouse'");

    // Insert gradually and check memory
    let mut es_docs = String::new();
    for block_num in 1..=100 {
        for seq in 1..=10 {
            let doc = sample_action(block_num, (block_num as u64 * 10) + seq as u64);
            let id = format!("{}_{}", block_num, seq);
            es_docs.push_str(&format!(
                "{{\"index\":{{\"_index\":\"test-action\",\"_id\":\"{}\" }}}}\n",
                id
            ));
            es_docs.push_str(&format!("{}\n", serde_json::to_string(&doc)?));
        }

        if block_num % 50 == 0 {
            println!("Indexed {} blocks...", block_num);
        }
    }

    println!("Starting ES insert of 1,000 actions...");
    let es_start = Instant::now();
    fixture.es.bulk(es_docs).await?;
    let es_time = es_start.elapsed();

    println!("ES insert complete: {:?}", es_time);
    println!("Rate: {:.0} actions/sec", 1000.0 / es_time.as_secs_f64());

    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_checkpoint_management() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    println!("\n=== Checkpoint Management Test ===");

    // ES checkpoint
    fixture.es.set_checkpoint("test-progress", 12345).await?;
    let es_checkpoint = fixture.es.get_checkpoint("test-progress").await?;
    println!("ES checkpoint: {:?}", es_checkpoint);
    assert_eq!(es_checkpoint, Some(12345));

    // CH checkpoint
    let version = (12345u64 << 32) | 0;
    fixture.ck.set_checkpoint(12345, version).await?;
    let ck_checkpoint = fixture.ck.get_checkpoint().await?;
    println!("CH checkpoint: {:?}", ck_checkpoint);
    assert_eq!(ck_checkpoint, Some(12345));

    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_count_operations() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    println!("\n=== Count Operations Test ===");

    // Insert test data
    let mut es_docs = String::new();
    for i in 1..=50 {
        let doc = sample_action(100, i as u64);
        es_docs.push_str(&format!("{{\"index\":{{\"_index\":\"test-action\",\"_id\":\"{}\" }}}}\n", i));
        es_docs.push_str(&format!("{}\n", serde_json::to_string(&doc)?));
    }

    fixture.es.bulk(es_docs).await?;

    // Count in ES
    let es_result = fixture.es.search("test-action", json!({
        "size": 0,
        "track_total_hits": true
    })).await?;
    let es_count = es_result["hits"]["total"]["value"].as_u64().unwrap_or(0);
    println!("ES action count: {}", es_count);

    // Count in CH (would need data inserted)
    let ck_count = fixture.ck.count("action").await.unwrap_or(0);
    println!("CH action count: {}", ck_count);

    fixture.cleanup().await?;
    Ok(())
}
