//! Integration test for ClickHouse indexing pipeline.
//!
//! Run with: cargo test --test integration_clickhouse -- --ignored --nocapture
//!
//! Requirements:
//! - ClickHouse running at http://localhost:8123

use anyhow::Result;
use hyperion::clickhouse::{ClickHouse, ClickHouseBatch, doc_to_row};
use hyperion::processor::{Doc, Op};
use serde_json::{json, Value};

#[tokio::test]
#[ignore]
async fn test_full_ingestion_pipeline() -> Result<()> {
    println!("\n=== Full ClickHouse Ingestion Pipeline Test ===\n");

    let ck = ClickHouse::new("http://localhost:8123", None, None);

    // Verify connection
    ck.ping().await?;
    println!("✓ Connected to ClickHouse");

    // Create tables
    ck.create_all().await?;
    println!("✓ Created all tables");

    // Create sample documents (simulating processor output)
    let mut batch = ClickHouseBatch::new();

    // Add 5 blocks and 50 actions
    for block_num in 1..=5 {
        // Create block document
        let block_doc = Doc {
            kind: "block",
            id: Some(block_num.to_string()),
            body: json!({
                "@timestamp": "2024-01-01T00:00:00Z",
                "block_num": block_num,
                "block_id": format!("{:064x}", block_num),
                "prev_id": format!("{:064x}", block_num - 1),
                "producer": "producer1",
                "schedule_version": 1,
                "trx_count": 10,
                "cpu_usage_us": 1000,
                "net_usage_words": 500
            }),
            op: Op::Index,
        };

        let version = ((block_num as u64) << 32) | 0;
        batch.push(&block_doc, version)?;

        // Create 10 action documents per block
        for seq in 1..=10 {
            let action_doc = Doc {
                kind: "action",
                id: Some(format!("{}-{}", block_num, seq)),
                body: json!({
                    "@timestamp": "2024-01-01T00:00:00Z",
                    "block_num": block_num,
                    "global_sequence": (block_num as u64 * 100) + seq as u64,
                    "block_id": format!("{:064x}", block_num),
                    "trx_id": format!("{:064x}", (block_num as u64 * 100) + seq as u64),
                    "producer": "producer1",
                    "act": {
                        "account": if seq % 2 == 0 { "eosio.token" } else { "alice" },
                        "name": if seq % 3 == 0 { "transfer" } else { "approve" },
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
                            "memo": "test"
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
                            "global_sequence": (block_num as u64 * 100) + seq as u64
                        }
                    ],
                    "cpu_usage_us": 100,
                    "net_usage_words": 50,
                    "action_ordinal": seq,
                    "creator_action_ordinal": 0,
                    "signatures": ["SIG_K1_123"]
                }),
                op: Op::Index,
            };

            let version = ((block_num as u64) << 32) | seq as u64;
            batch.push(&action_doc, version)?;
        }
    }

    println!("\n📊 Batch created:");
    println!("   Total documents: {}", batch.count);
    println!("   Max block: {}", batch.max_block);
    println!("   Tables: {:?}", batch.rows_by_table.keys().collect::<Vec<_>>());

    // Insert batch
    println!("\n📝 Inserting batch...");
    let start = std::time::Instant::now();

    for table in &["block", "action"] {
        let data = batch.to_tab_separated(table);
        if !data.is_empty() {
            ck.insert_tab_separated(table, data).await?;
            println!("   ✓ Inserted into {}", table);
        }
    }

    let elapsed = start.elapsed();
    println!("\n✓ Batch inserted in {:?}", elapsed);
    println!("  Rate: {:.0} docs/sec", batch.count as f64 / elapsed.as_secs_f64());

    // Verify counts
    println!("\n📊 Verifying data:");
    let block_count = ck.count("block").await?;
    println!("   Blocks in CH: {}", block_count);
    assert_eq!(block_count, 5);

    let action_count = ck.count("action").await?;
    println!("   Actions in CH: {}", action_count);
    assert_eq!(action_count, 50);

    // Test checkpoint
    println!("\n🔖 Testing checkpoint:");
    let version = (5u64 << 32) | 0;
    ck.set_checkpoint(5, version).await?;
    let checkpoint = ck.get_checkpoint().await?;
    println!("   Checkpoint: {:?}", checkpoint);
    assert_eq!(checkpoint, Some(5));

    // Test queries
    println!("\n🔍 Testing queries:");
    let max_block = ck.max_block_num("block").await?;
    println!("   Max block_num: {:?}", max_block);
    assert_eq!(max_block, Some(5));

    // Test action query with FINAL
    let sql = "SELECT count() FROM action FINAL WHERE has(notified, 'alice')";
    let result = ck.query(sql).await?;
    let count: u64 = result.trim().parse()?;
    println!("   Actions notifying alice: {}", count);
    assert_eq!(count, 50); // all actions notify alice

    println!("\n✅ All tests passed!");

    // Cleanup
    ck.drop_all().await?;
    println!("\n🧹 Cleaned up ClickHouse");

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_row_serialization() -> Result<()> {
    println!("\n=== Row Serialization Test ===\n");

    // Test action row serialization
    let action_doc = Doc {
        kind: "action",
        id: Some("test".to_string()),
        body: json!({
            "@timestamp": "2024-01-01T00:00:00Z",
            "block_num": 100,
            "global_sequence": 1000,
            "block_id": "deadbeef",
            "trx_id": "cafebabe",
            "producer": "producer1",
            "act": {
                "account": "eosio.token",
                "name": "transfer",
                "authorization": [
                    { "actor": "alice", "permission": "active" }
                ],
                "data": { "from": "alice", "to": "bob", "amount": 100 },
                "hex_data": "abcd1234"
            },
            "@transfer": {
                "from": "alice",
                "to": "bob",
                "amount": 100.0,
                "symbol": "EOS",
                "memo": "test"
            },
            "notified": ["alice", "bob"],
            "receipts": [
                { "receiver": "alice", "global_sequence": 1000 }
            ],
            "cpu_usage_us": 100,
            "net_usage_words": 50,
            "action_ordinal": 1,
            "creator_action_ordinal": 0,
            "signatures": ["SIG_K1_123"]
        }),
        op: Op::Index,
    };

    let version = (100u64 << 32) | 0;
    let row = doc_to_row(&action_doc, version)?;

    println!("Action row fields: {}", row.len());
    println!("Block num: {}", row[0]);
    println!("Global seq: {}", row[1]);
    println!("Act account: {}", row[6]);
    println!("Act name: {}", row[7]);
    println!("Version: {}", row[row.len() - 1]);

    // Verify TabSeparated format
    assert_eq!(row[0], "100"); // block_num
    assert_eq!(row[6], "eosio.token"); // act_account
    assert_eq!(row[7], "transfer"); // act_name
    assert_eq!(row[row.len() - 1], version.to_string()); // version

    println!("\n✅ Row serialization test passed!");

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_batch_operations() -> Result<()> {
    println!("\n=== Batch Operations Test ===\n");

    let ck = ClickHouse::new("http://localhost:8123", None, None);
    ck.create_all().await?;

    let mut batch = ClickHouseBatch::new();

    // Create multiple blocks with different types of documents
    for block in 1..=3 {
        // Block document
        let block_doc = Doc {
            kind: "block",
            id: Some(block.to_string()),
            body: json!({
                "@timestamp": "2024-01-01T00:00:00Z",
                "block_num": block,
                "block_id": format!("{:064x}", block),
                "prev_id": format!("{:064x}", block - 1),
                "producer": "producer1",
                "schedule_version": 1,
                "trx_count": 5,
                "cpu_usage_us": 1000,
                "net_usage_words": 500
            }),
            op: Op::Index,
        };

        let version = ((block as u64) << 32) | 0;
        batch.push(&block_doc, version)?;

        // Token snapshot document
        let token_doc = Doc {
            kind: "token",
            id: Some(format!("eosio.token-{}-EOS", block)),
            body: json!({
                "block_num": block,
                "code": "eosio.token",
                "scope": "alice",
                "symbol": "EOS",
                "precision": 4,
                "amount": 1000.0
            }),
            op: Op::Index,
        };

        batch.push(&token_doc, version)?;

        // Permission snapshot document
        let perm_doc = Doc {
            kind: "perm",
            id: Some(format!("alice-active-{}", block)),
            body: json!({
                "block_num": block,
                "owner": "alice",
                "name": "active",
                "parent": "owner",
                "last_updated": "2024-01-01T00:00:00Z",
                "keys": ["PUB_K1_123"],
                "accounts": [],
                "threshold": 1
            }),
            op: Op::Index,
        };

        batch.push(&perm_doc, version)?;
    }

    println!("📊 Multi-table batch created:");
    println!("   Documents: {}", batch.count);
    println!("   Tables: {:?}", batch.rows_by_table.keys().collect::<Vec<_>>());

    // Insert
    for table in &["block", "token", "perm"] {
        let data = batch.to_tab_separated(table);
        if !data.is_empty() {
            ck.insert_tab_separated(table, data).await?;
            let count = ck.count(table).await?;
            println!("   ✓ {} inserted {} rows", table, count);
        }
    }

    // Verify
    assert_eq!(ck.count("block").await?, 3);
    assert_eq!(ck.count("token").await?, 3);
    assert_eq!(ck.count("perm").await?, 3);

    println!("\n✅ Batch operations test passed!");

    ck.drop_all().await?;
    Ok(())
}
