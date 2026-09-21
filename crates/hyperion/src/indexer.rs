//! The indexing pipeline: SHIP reader task → processor → Elasticsearch
//! bulk writer. Hyperion uses RabbitMQ between these stages; here they are
//! in-process tasks connected by bounded channels, with SHIP's own
//! credit-based flow control providing end-to-end backpressure.

use crate::abis::AbiCache;
use crate::config::Config;
use crate::elastic::{index_definitions, Elastic};
use crate::processor::{Doc, Op, Processor};
use anyhow::{Context, Result};
use serde::Serialize;
use ship::{GetBlocksRequest, GetBlocksResult, ShipClient, ShipResult};
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::Instant;

pub async fn run(config: Config) -> Result<()> {
    anyhow::ensure!(
        config.indexer.max_messages_in_flight > 0,
        "max_messages_in_flight must be positive"
    );
    anyhow::ensure!(config.indexer.batch_size > 0, "batch_size must be positive");
    anyhow::ensure!(
        config.indexer.batch_max_bytes > 0,
        "batch_max_bytes must be positive"
    );
    anyhow::ensure!(
        config.indexer.flush_interval_ms > 0,
        "flush_interval_ms must be positive"
    );
    let es = Elastic::new(&config.elasticsearch);
    let info = es.ping().await.context("cannot reach elasticsearch")?;
    tracing::info!(version = %info["version"]["number"], "connected to elasticsearch");

    for (kind, body) in
        index_definitions(config.elasticsearch.shards, config.elasticsearch.replicas)
    {
        es.create_index(&config.index(kind), body).await?;
    }

    let start_block = resolve_start_block(&config, &es).await?;
    let stop_block = config.indexer.stop_block;

    let mut client = ShipClient::connect(&config.chain.ship)
        .await
        .with_context(|| format!("cannot connect to SHIP at {}", config.chain.ship))?;
    let status = client.get_status().await?;
    tracing::info!(
        head = status.head.block_num,
        lib = status.last_irreversible.block_num,
        trace_begin = status.trace_begin_block,
        trace_end = status.trace_end_block,
        chain_id = status.chain_id.as_deref().unwrap_or("unknown"),
        "connected to state history"
    );
    if start_block < status.trace_begin_block {
        tracing::warn!(
            start_block,
            trace_begin = status.trace_begin_block,
            "start block predates available trace history; traces will be empty until then"
        );
    }

    client
        .request_blocks(&GetBlocksRequest {
            start_block_num: start_block,
            // end_block_num is exclusive.
            end_block_num: if stop_block > 0 {
                stop_block + 1
            } else {
                u32::MAX
            },
            max_messages_in_flight: config.indexer.max_messages_in_flight,
            irreversible_only: false,
            fetch_block: config.indexer.fetch_block,
            fetch_traces: config.indexer.fetch_traces,
            fetch_deltas: config.indexer.fetch_deltas,
        })
        .await?;
    tracing::info!(start_block, stop_block, "requested block stream");

    // Reader task: pull frames off the socket, refill SHIP credit as the
    // channel accepts each block (bounded channel = backpressure).
    let (tx, rx) = mpsc::channel::<Box<GetBlocksResult>>(
        config.indexer.max_messages_in_flight.max(1) as usize,
    );
    let reader = async move {
        loop {
            match client.next_result().await {
                Ok(ShipResult::Blocks(block)) => {
                    let done = stop_block > 0
                        && block
                            .this_block
                            .as_ref()
                            .is_some_and(|b| b.block_num >= stop_block);
                    if tx.send(block).await.is_err() {
                        return Ok(());
                    }
                    if done {
                        return Ok(());
                    }
                    if let Err(e) = client.ack_blocks(1).await {
                        return Err(anyhow::Error::from(e).context("failed to ack blocks"));
                    }
                }
                Ok(ShipResult::Status(_)) => continue,
                Err(ship::ShipError::Closed) => return Ok(()),
                Err(e) => return Err(anyhow::Error::from(e).context("state history stream")),
            }
        }
    };

    // Keep writes ordered: concurrent requests could let older token balances,
    // permission changes, or fork replacements overwrite newer documents.
    // Two queued batches bound how far processing can run ahead of the writer.
    let (batch_tx, batch_rx) = mpsc::channel(2);
    // JoinSet aborts the remaining stages on error or cancellation, so neither
    // the socket reader nor pending writes can outlive this indexing run.
    let mut stages = tokio::task::JoinSet::new();
    stages.spawn(reader);
    stages.spawn(async move { process_blocks(&config, rx, batch_tx).await });
    stages.spawn(async move { write_batches(&es, batch_rx).await });
    while let Some(result) = stages.join_next().await {
        result??;
    }
    tracing::info!("indexer finished");
    Ok(())
}

async fn process_blocks(
    config: &Config,
    mut rx: mpsc::Receiver<Box<GetBlocksResult>>,
    tx: mpsc::Sender<BulkBatch>,
) -> Result<()> {
    let system_account: antelope::Name = config
        .chain
        .system_account
        .parse()
        .map_err(|e| anyhow::anyhow!("bad chain.system_account: {e}"))?;
    let processor = Processor::new(&config.indexer.skip_actions, system_account);
    let mut abis = AbiCache::new(config.chain.client());
    let indices: HashMap<_, _> = ["action", "block", "delta", "abi", "perm", "token"]
        .into_iter()
        .map(|kind| (kind, config.index(kind)))
        .collect();
    let mut batch = BulkBatch::default();
    let mut last_flush = Instant::now();
    let mut last_report = Instant::now();
    let mut blocks_since_report = 0u64;
    let mut last_block = 0u32;
    let flush_interval = Duration::from_millis(config.indexer.flush_interval_ms);

    loop {
        let block = tokio::select! {
            block = rx.recv() => block,
            _ = tokio::time::sleep_until(last_flush + flush_interval), if batch.count > 0 => {
                tx.send(std::mem::take(&mut batch)).await?;
                last_flush = Instant::now();
                continue;
            }
        };
        let Some(block) = block else { break };

        if let Some(this_block) = &block.this_block {
            if last_block > 0 && this_block.block_num <= last_block {
                tracing::warn!(
                    block_num = this_block.block_num,
                    last_block,
                    "microfork: re-received earlier block; overwriting"
                );
            }
            last_block = this_block.block_num;
        }

        for doc in processor.process_block(&block, &mut abis).await? {
            batch.push(&indices[doc.kind], &doc)?;
        }
        blocks_since_report += 1;

        if batch.count > 0
            && (batch.count >= config.indexer.batch_size
                || batch.body.len() >= config.indexer.batch_max_bytes
                || last_flush.elapsed() >= flush_interval)
        {
            tx.send(std::mem::take(&mut batch)).await?;
            last_flush = Instant::now();
        }
        if last_report.elapsed() >= Duration::from_secs(10) {
            let rate = blocks_since_report as f64 / last_report.elapsed().as_secs_f64();
            tracing::info!(last_block, rate = format!("{rate:.0} blocks/s"), "indexing");
            last_report = Instant::now();
            blocks_since_report = 0;
        }
    }

    if batch.count > 0 {
        tx.send(batch).await?;
    }
    Ok(())
}

async fn resolve_start_block(config: &Config, es: &Elastic) -> Result<u32> {
    if config.indexer.start_block > 0 {
        return Ok(config.indexer.start_block);
    }
    // Resume after the highest indexed block; block index first, actions as
    // fallback for deployments that disable fetch_block.
    for kind in ["block", "action"] {
        if let Some(max) = es.max_block_num(&config.index(kind)).await? {
            tracing::info!(
                resume_from = max + 1,
                index = kind,
                "resuming from last indexed block"
            );
            return Ok(max + 1);
        }
    }
    Ok(1)
}

#[derive(Default)]
struct BulkBatch {
    body: Vec<u8>,
    count: usize,
}

#[derive(Serialize)]
struct BulkMetadata<'a> {
    #[serde(rename = "_index")]
    index: &'a str,
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    id: Option<&'a str>,
}

impl BulkBatch {
    fn push(&mut self, index: &str, doc: &Doc) -> Result<()> {
        let meta = BulkMetadata {
            index,
            id: doc.id.as_deref(),
        };
        match doc.op {
            Op::Index => {
                self.body.extend_from_slice(b"{\"index\":");
                serde_json::to_writer(&mut self.body, &meta)?;
                self.body.extend_from_slice(b"}\n");
                serde_json::to_writer(&mut self.body, &doc.body)?;
                self.body.push(b'\n');
            }
            Op::Delete => {
                anyhow::ensure!(meta.id.is_some(), "delete requires a document ID");
                self.body.extend_from_slice(b"{\"delete\":");
                serde_json::to_writer(&mut self.body, &meta)?;
                self.body.extend_from_slice(b"}\n");
            }
        }
        self.count += 1;
        Ok(())
    }
}

async fn write_batches(es: &Elastic, mut rx: mpsc::Receiver<BulkBatch>) -> Result<()> {
    while let Some(batch) = rx.recv().await {
        let count = batch.count;
        let bytes = batch.body.len();
        let started = Instant::now();
        let failed = es.bulk(batch.body).await?;
        // Do not submit newer batches after a failed write: doing so would
        // advance the resume position past documents that were never indexed.
        anyhow::ensure!(
            failed == 0,
            "bulk indexing failed for {failed} of {count} documents"
        );
        tracing::debug!(
            count,
            bytes,
            elapsed_ms = started.elapsed().as_millis(),
            "flushed batch"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    fn block(number: u32) -> Box<GetBlocksResult> {
        // Empty signed block: fixed header, no producer schedule or header
        // extensions, K1 signature, and zero transaction receipts.
        let mut header = vec![0; 4 + 8 + 2 + 32 * 3 + 4];
        header.extend_from_slice(&[0, 0, 0]);
        header.extend_from_slice(&[0; 65]);
        header.push(0);
        let position = ship::BlockPosition {
            block_num: number,
            block_id: "00".repeat(32),
        };
        Box::new(GetBlocksResult {
            head: position.clone(),
            last_irreversible: position.clone(),
            this_block: Some(position),
            prev_block: None,
            block: Some(header),
            traces: None,
            deltas: None,
        })
    }

    fn config() -> Config {
        toml::from_str(
            r#"
            [chain]
            name = "test"
            http = "http://127.0.0.1:1"
            ship = "ws://127.0.0.1:1"
            [indexer]
            flush_interval_ms = 50
        "#,
        )
        .unwrap()
    }

    #[tokio::test(start_paused = true)]
    async fn partial_batch_flush_deadline_does_not_reset_on_arrival() {
        let (input, rx) = mpsc::channel(2);
        let (tx, mut output) = mpsc::channel(2);
        let processor = tokio::spawn(async move { process_blocks(&config(), rx, tx).await });
        input.send(block(1)).await.unwrap();
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(40)).await;
        input.send(block(2)).await.unwrap();
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(10)).await;
        let batch = tokio::time::timeout(Duration::from_millis(2), output.recv())
            .await
            .expect("partial batch deadline was postponed")
            .unwrap();
        assert_eq!(batch.count, 2);
        drop(input);
        processor.await.unwrap().unwrap();
        assert!(output.recv().await.is_none());
    }

    #[tokio::test]
    async fn writer_stops_before_submitting_queued_batches_after_failure() {
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };
        let requests = Arc::new(AtomicUsize::new(0));
        let counter = requests.clone();
        let app = axum::Router::new().route(
            "/_bulk",
            axum::routing::post(move || {
                counter.fetch_add(1, Ordering::SeqCst);
                async {
                    axum::Json(json!({"errors": true, "items": [
                        {"index": {"status": 429, "error": {"type": "rejected"}}}
                    ]}))
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = config();
        config.elasticsearch.url = format!("http://{}", listener.local_addr().unwrap());
        let mut servers = tokio::task::JoinSet::new();
        servers.spawn(async move { axum::serve(listener, app).await.unwrap() });
        let (tx, rx) = mpsc::channel(2);
        for _ in 0..2 {
            tx.send(BulkBatch {
                count: 1,
                body: b"{}\n".to_vec(),
            })
            .await
            .unwrap();
        }
        drop(tx);
        let error = write_batches(&Elastic::new(&config.elasticsearch), rx)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("bulk indexing failed"));
        assert_eq!(requests.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn serializes_index_and_delete_operations_with_escaped_metadata() {
        let mut batch = BulkBatch::default();
        let mut doc = Doc {
            kind: "action",
            id: Some("quoted\"id\n".into()),
            body: json!({"memo": "line one\nline two", "value": 42}),
            op: Op::Index,
        };
        batch.push("test-action", &doc).unwrap();
        doc.id = None;
        batch.push("test-action", &doc).unwrap();
        doc.op = Op::Delete;
        doc.id = Some("balance".into());
        batch.push("test-token", &doc).unwrap();
        assert_eq!(batch.count, 3);
        assert!(batch.body.ends_with(b"\n"));
        let body = String::from_utf8(batch.body).unwrap();
        let lines: Vec<Value> = body
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(
            lines,
            vec![
                json!({"index": {"_index": "test-action", "_id": "quoted\"id\n"}}),
                doc.body.clone(),
                json!({"index": {"_index": "test-action"}}),
                doc.body,
                json!({"delete": {"_index": "test-token", "_id": "balance"}}),
            ]
        );
    }
}
