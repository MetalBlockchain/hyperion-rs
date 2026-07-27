//! The indexing pipeline: SHIP reader task → processor → Elasticsearch
//! bulk writer. Hyperion uses RabbitMQ between these stages; here they are
//! in-process tasks connected by a bounded channel, with SHIP's own
//! credit-based flow control providing end-to-end backpressure.

use crate::abis::AbiCache;
use crate::config::Config;
use crate::elastic::{index_definitions, Elastic};
use crate::processor::{Doc, Op, Processor};
use anyhow::{Context, Result};
use ship::{GetBlocksRequest, GetBlocksResult, ShipClient, ShipResult};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

pub async fn run(config: Config) -> Result<()> {
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
    let (tx, mut rx) = mpsc::channel::<Box<GetBlocksResult>>(
        config.indexer.max_messages_in_flight.max(1) as usize,
    );
    let reader = tokio::spawn(async move {
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
    });

    let system_account: antelope::Name = config
        .chain
        .system_account
        .parse()
        .map_err(|e| anyhow::anyhow!("bad chain.system_account: {e}"))?;
    let processor = Processor::new(&config.indexer.skip_actions, system_account);
    let mut abis = AbiCache::new(config.chain.client());
    let mut batch: Vec<Doc> = Vec::new();
    let mut last_flush = Instant::now();
    let mut last_report = Instant::now();
    let mut blocks_since_report = 0u64;
    let mut last_block = 0u32;
    let flush_interval = Duration::from_millis(config.indexer.flush_interval_ms);

    loop {
        let block = tokio::select! {
            block = rx.recv() => block,
            _ = tokio::time::sleep(flush_interval), if !batch.is_empty() => {
                flush(&es, &config, &mut batch).await?;
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

        batch.extend(processor.process_block(&block, &mut abis).await?);
        blocks_since_report += 1;

        if batch.len() >= config.indexer.batch_size || last_flush.elapsed() >= flush_interval {
            flush(&es, &config, &mut batch).await?;
            last_flush = Instant::now();
        }
        if last_report.elapsed() >= Duration::from_secs(10) {
            let rate = blocks_since_report as f64 / last_report.elapsed().as_secs_f64();
            tracing::info!(last_block, rate = format!("{rate:.0} blocks/s"), "indexing");
            last_report = Instant::now();
            blocks_since_report = 0;
        }
    }

    flush(&es, &config, &mut batch).await?;
    reader.await??;
    tracing::info!(last_block, "indexer finished");
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

async fn flush(es: &Elastic, config: &Config, batch: &mut Vec<Doc>) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let mut body = String::with_capacity(batch.len() * 256);
    for doc in batch.iter() {
        let index = config.index(doc.kind);
        match doc.op {
            Op::Index => {
                let meta = match &doc.id {
                    Some(id) => serde_json::json!({"index": {"_index": index, "_id": id}}),
                    None => serde_json::json!({"index": {"_index": index}}),
                };
                body.push_str(&meta.to_string());
                body.push('\n');
                body.push_str(&doc.body.to_string());
                body.push('\n');
            }
            Op::Delete => {
                let id = doc.id.as_deref().unwrap_or_default();
                body.push_str(
                    &serde_json::json!({"delete": {"_index": index, "_id": id}}).to_string(),
                );
                body.push('\n');
            }
        }
    }
    let count = batch.len();
    batch.clear();
    let failed = es.bulk(body).await?;
    if failed > 0 {
        tracing::error!(failed, count, "bulk indexing had failures");
    } else {
        tracing::debug!(count, "flushed batch");
    }
    Ok(())
}
