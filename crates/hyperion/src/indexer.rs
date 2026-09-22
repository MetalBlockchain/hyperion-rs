//! The indexing pipeline: SHIP reader → parallel raw decoders → ordered
//! processor → Elasticsearch bulk writer. Hyperion uses RabbitMQ between
//! these stages; here they are
//! in-process tasks connected by bounded channels, with SHIP's own
//! credit-based flow control providing end-to-end backpressure.

use crate::abis::AbiCache;
use crate::config::Config;
use crate::elastic::{index_definitions, Elastic};
use crate::processor::{DecodedBlock, Doc, Op, Processor};
use anyhow::{Context, Result};
use serde::Serialize;
use ship::{GetBlocksRequest, GetBlocksResult, ShipClient, ShipResult};
use std::collections::{BTreeMap, HashMap};
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

    // Batches queue ahead of the writer so it's never starved between
    // flushes; the writer itself may run several bulk requests concurrently
    // (see write_batches) since each document's external version makes
    // out-of-order completion safe.
    let (batch_tx, batch_rx) = mpsc::channel(2);
    // JoinSet aborts the remaining stages on error or cancellation, so neither
    // the socket reader nor pending writes can outlive this indexing run.
    let mut stages = tokio::task::JoinSet::new();
    stages.spawn(reader);
    let input = if config.indexer.decode_workers == 0 {
        BlockInput::Raw(rx)
    } else {
        let (decoded_tx, decoded_rx) = mpsc::channel(2);
        let workers = config.indexer.decode_workers;
        stages.spawn(decode_blocks(rx, decoded_tx, workers, DecodedBlock::decode));
        BlockInput::Decoded(decoded_rx)
    };
    tracing::info!(
        decode_workers = config.indexer.decode_workers,
        writer_concurrency = config.indexer.writer_concurrency,
        "started indexing pipeline"
    );
    let progress_index = config.index("progress");
    let writer_concurrency = config.indexer.writer_concurrency;
    stages.spawn(async move { process_blocks(&config, input, batch_tx).await });
    stages.spawn(
        async move { write_batches(&es, batch_rx, writer_concurrency, &progress_index).await },
    );
    while let Some(result) = stages.join_next().await {
        result??;
    }
    tracing::info!("indexer finished");
    Ok(())
}

enum BlockInput {
    Raw(mpsc::Receiver<Box<GetBlocksResult>>),
    Decoded(mpsc::Receiver<DecodedBlock>),
}

impl BlockInput {
    async fn recv(&mut self) -> Option<Result<DecodedBlock>> {
        match self {
            Self::Raw(rx) => rx.recv().await.map(|raw| DecodedBlock::decode(&raw)),
            Self::Decoded(rx) => rx.recv().await.map(Ok),
        }
    }
}

/// Bound running jobs AND completed results waiting for an earlier block.
/// Sorting by stream ordinal (not block number) preserves microfork replay.
async fn decode_blocks<F>(
    mut rx: mpsc::Receiver<Box<GetBlocksResult>>,
    tx: mpsc::Sender<DecodedBlock>,
    workers: usize,
    decode: F,
) -> Result<()>
where
    F: Fn(&GetBlocksResult) -> Result<DecodedBlock> + Send + Sync + Clone + 'static,
{
    anyhow::ensure!(workers > 0, "decoder pool needs at least one worker");
    let mut jobs = tokio::task::JoinSet::new();
    let mut ready = BTreeMap::new();
    let mut submitted = 0u64;
    let mut emitted = 0u64;
    let mut closed = false;
    loop {
        if let Some(decoded) = ready.remove(&emitted) {
            tx.send(decoded?).await?;
            emitted += 1;
            continue;
        }
        if closed && jobs.is_empty() {
            return Ok(());
        }
        tokio::select! {
            raw = rx.recv(), if !closed && jobs.len() + ready.len() < workers => {
                match raw {
                    Some(raw) => {
                        let ordinal = submitted;
                        submitted += 1;
                        let decode = decode.clone();
                        jobs.spawn_blocking(move || {
                            let block_num = raw.this_block.as_ref().map(|b| b.block_num);
                            let decoded = decode(&raw)
                                .with_context(|| format!("decoding SHIP block {block_num:?}"));
                            (ordinal, decoded)
                        });
                    }
                    None => closed = true,
                }
            }
            result = jobs.join_next(), if !jobs.is_empty() => {
                let (ordinal, decoded) = result.expect("nonempty decoder jobs")
                    .context("raw block decoder task failed")?;
                ready.insert(ordinal, decoded);
            }
        }
    }
}

async fn process_blocks(
    config: &Config,
    mut rx: BlockInput,
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
        let block = block?;

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

        let docs = processor.process_decoded(&block, &mut abis).await?;
        for (seq, doc) in docs.into_iter().enumerate() {
            // High bits = block, low bits = position within the block: always
            // increases across blocks, and disambiguates multiple updates to
            // the same entity (e.g. a permission touched twice) within one.
            let version = (u64::from(last_block) << 32) | seq as u64;
            batch.push(&indices[doc.kind], &doc, version)?;
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
    // The checkpoint only advances once batches complete contiguously (see
    // write_batches), so it's safe to trust even though writes themselves
    // may complete out of order. Prefer it over the raw aggregation below,
    // which can't tell "durably confirmed" apart from "visible but a lower
    // block is still in flight or failed".
    if let Some(checkpoint) = es.get_checkpoint(&config.index("progress")).await? {
        tracing::info!(
            resume_from = checkpoint + 1,
            index = "progress",
            "resuming from last confirmed checkpoint"
        );
        return Ok(checkpoint + 1);
    }
    // Deployments predating the checkpoint doc: fall back to the highest
    // indexed block; block index first, actions as fallback for deployments
    // that disable fetch_block.
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
    /// Highest block any document in this batch belongs to, so the writer
    /// can advance the resume checkpoint once the batch is durably written.
    max_block: u32,
}

#[derive(Serialize)]
struct BulkMetadata<'a> {
    #[serde(rename = "_index")]
    index: &'a str,
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    id: Option<&'a str>,
    version_type: &'static str,
    version: u64,
}

impl BulkBatch {
    /// `version`'s high 32 bits are the document's block number (see the
    /// call site in `process_blocks`), used both as Elasticsearch's external
    /// version — so a write that arrives out of order is rejected rather
    /// than silently overwriting a newer document — and to track how far
    /// this batch's content reaches for the resume checkpoint.
    fn push(&mut self, index: &str, doc: &Doc, version: u64) -> Result<()> {
        let meta = BulkMetadata {
            index,
            id: doc.id.as_deref(),
            version_type: "external",
            version,
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
        self.max_block = self.max_block.max((version >> 32) as u32);
        Ok(())
    }
}

/// Runs up to `concurrency` bulk requests at once. Each document's external
/// version (`BulkBatch::push`) makes out-of-order completion safe at the
/// Elasticsearch level, but batches are still confirmed in submission order
/// here so `progress_index`'s checkpoint never advances past one that's
/// still in flight or that failed — even though a later batch may already
/// have landed on the wire by the time an earlier one is confirmed.
async fn write_batches(
    es: &Elastic,
    mut rx: mpsc::Receiver<BulkBatch>,
    concurrency: usize,
    progress_index: &str,
) -> Result<()> {
    anyhow::ensure!(concurrency > 0, "writer_concurrency must be at least 1");
    let mut jobs = tokio::task::JoinSet::new();
    let mut ready: BTreeMap<u64, (u32, Result<()>)> = BTreeMap::new();
    let mut submitted = 0u64;
    let mut completed = 0u64;
    let mut checkpoint = 0u32;
    let mut closed = false;
    loop {
        if let Some((max_block, result)) = ready.remove(&completed) {
            result?;
            completed += 1;
            if max_block > checkpoint {
                checkpoint = max_block;
                es.set_checkpoint(progress_index, checkpoint).await?;
            }
            continue;
        }
        if closed && jobs.is_empty() {
            return Ok(());
        }
        tokio::select! {
            batch = rx.recv(), if !closed && jobs.len() + ready.len() < concurrency => {
                match batch {
                    Some(batch) => {
                        let ordinal = submitted;
                        submitted += 1;
                        let max_block = batch.max_block;
                        let count = batch.count;
                        let bytes = batch.body.len();
                        let es = es.clone();
                        jobs.spawn(async move {
                            let started = Instant::now();
                            let result = es.bulk(batch.body).await.and_then(|failed| {
                                anyhow::ensure!(
                                    failed == 0,
                                    "bulk indexing failed for {failed} of {count} documents"
                                );
                                Ok(())
                            });
                            if result.is_ok() {
                                tracing::debug!(
                                    count,
                                    bytes,
                                    elapsed_ms = started.elapsed().as_millis(),
                                    "flushed batch"
                                );
                            }
                            (ordinal, max_block, result)
                        });
                    }
                    None => closed = true,
                }
            }
            result = jobs.join_next(), if !jobs.is_empty() => {
                let (ordinal, max_block, result) = result
                    .expect("nonempty writer jobs")
                    .context("bulk writer task failed")?;
                ready.insert(ordinal, (max_block, result));
            }
        }
    }
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

    #[tokio::test]
    async fn decoder_pool_is_parallel_bounded_and_preserves_fork_arrival_order() {
        use std::sync::{Arc, Mutex};
        let (input, rx) = mpsc::channel(4);
        let (tx, mut output) = mpsc::channel(1);
        let (started, mut events) = mpsc::unbounded_channel();
        let (release, wait) = std::sync::mpsc::channel();
        let wait = Arc::new(Mutex::new(wait));
        let decoder = move |raw: &GetBlocksResult| {
            let number = raw.this_block.as_ref().unwrap().block_num;
            started.send(number).unwrap();
            if number == 100 {
                wait.lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap();
            }
            DecodedBlock::decode(raw)
        };
        let pool = tokio::spawn(decode_blocks(rx, tx, 2, decoder));
        // A backwards jump and a repeated height must not be sorted by height.
        for number in [100, 101, 99, 99] {
            input.send(block(number)).await.unwrap();
        }
        let mut first = Vec::new();
        for _ in 0..2 {
            first.push(
                tokio::time::timeout(Duration::from_secs(5), events.recv())
                    .await
                    .unwrap()
                    .unwrap(),
            );
        }
        first.sort_unstable();
        assert_eq!(
            first,
            vec![100, 101],
            "second worker must run while first is blocked"
        );
        assert!(
            output.try_recv().is_err(),
            "later block must not overtake earlier block"
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(20), events.recv())
                .await
                .is_err(),
            "completed results must count towards the outstanding work limit"
        );
        assert_eq!(input.capacity(), 2);
        release.send(()).unwrap();
        drop(input);
        let mut numbers = Vec::new();
        while let Some(decoded) = tokio::time::timeout(Duration::from_secs(5), output.recv())
            .await
            .unwrap()
        {
            numbers.push(decoded.this_block.unwrap().block_num);
        }
        assert_eq!(numbers, vec![100, 101, 99, 99]);
        pool.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn decoder_pool_reports_malformed_payloads_and_panics() {
        for malformed_traces in [true, false] {
            let (input, rx) = mpsc::channel(1);
            let (tx, mut output) = mpsc::channel(1);
            let mut raw = block(42);
            if malformed_traces {
                raw.traces = Some(vec![255]);
            } else {
                raw.deltas = Some(vec![255]);
            }
            input.send(raw).await.unwrap();
            drop(input);
            let error = decode_blocks(rx, tx, 2, DecodedBlock::decode)
                .await
                .unwrap_err();
            assert!(
                error.to_string().contains("decoding SHIP block Some(42)"),
                "{error}"
            );
            assert!(output.recv().await.is_none());
        }
        let (input, rx) = mpsc::channel(1);
        let (tx, mut output) = mpsc::channel(1);
        input.send(block(42)).await.unwrap();
        drop(input);
        let error = decode_blocks(rx, tx, 2, |_| panic!("injected decoder panic"))
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("raw block decoder task failed"),
            "{error}"
        );
        assert!(output.recv().await.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn partial_batch_flush_deadline_does_not_reset_on_arrival() {
        let (input, rx) = mpsc::channel(2);
        let (tx, mut output) = mpsc::channel(2);
        let processor =
            tokio::spawn(async move { process_blocks(&config(), BlockInput::Raw(rx), tx).await });
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
        for i in 0..2 {
            tx.send(BulkBatch {
                count: 1,
                body: b"{}\n".to_vec(),
                max_block: i + 1,
            })
            .await
            .unwrap();
        }
        drop(tx);
        // concurrency = 1 reproduces the old fully-serial writer: the second
        // batch must never even be submitted once the first has failed.
        let error = write_batches(
            &Elastic::new(&config.elasticsearch),
            rx,
            1,
            "unused-progress",
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("bulk indexing failed"));
        assert_eq!(requests.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn checkpoint_never_advances_past_a_failed_batch_even_if_a_later_one_lands_first() {
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };
        let checkpoint_puts = Arc::new(AtomicUsize::new(0));
        let puts = checkpoint_puts.clone();
        let app = axum::Router::new()
            .route(
                "/_bulk",
                axum::routing::post(|body: axum::body::Bytes| async move {
                    if body.starts_with(b"fail") {
                        axum::Json(json!({"errors": true, "items": [
                            {"index": {"status": 429, "error": {"type": "rejected"}}}
                        ]}))
                    } else {
                        axum::Json(json!({"errors": false}))
                    }
                }),
            )
            .route(
                "/progress-index/_doc/checkpoint",
                axum::routing::put(move || {
                    puts.fetch_add(1, Ordering::SeqCst);
                    async { axum::Json(json!({"result": "updated"})) }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = config();
        config.elasticsearch.url = format!("http://{}", listener.local_addr().unwrap());
        let mut servers = tokio::task::JoinSet::new();
        servers.spawn(async move { axum::serve(listener, app).await.unwrap() });
        let (tx, rx) = mpsc::channel(2);
        // Ordinal 0 fails, ordinal 1 (a later block) would succeed on its
        // own — with concurrency > 1 both are in flight before either
        // completes, so ordinal 1 may well land on the wire first.
        tx.send(BulkBatch {
            count: 1,
            body: b"fail\n".to_vec(),
            max_block: 1,
        })
        .await
        .unwrap();
        tx.send(BulkBatch {
            count: 1,
            body: b"ok\n".to_vec(),
            max_block: 2,
        })
        .await
        .unwrap();
        drop(tx);
        let error = write_batches(
            &Elastic::new(&config.elasticsearch),
            rx,
            4,
            "progress-index",
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("bulk indexing failed"));
        assert_eq!(
            checkpoint_puts.load(Ordering::SeqCst),
            0,
            "checkpoint must not advance while an earlier batch is unresolved or failed"
        );
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
        batch.push("test-action", &doc, 7).unwrap();
        doc.id = None;
        batch.push("test-action", &doc, 7).unwrap();
        doc.op = Op::Delete;
        doc.id = Some("balance".into());
        batch.push("test-token", &doc, (2u64 << 32) | 1).unwrap();
        assert_eq!(batch.count, 3);
        assert_eq!(batch.max_block, 2);
        assert!(batch.body.ends_with(b"\n"));
        let body = String::from_utf8(batch.body).unwrap();
        let lines: Vec<Value> = body
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(
            lines,
            vec![
                json!({
                    "index": {"_index": "test-action", "_id": "quoted\"id\n",
                              "version_type": "external", "version": 7}
                }),
                doc.body.clone(),
                json!({
                    "index": {"_index": "test-action",
                              "version_type": "external", "version": 7}
                }),
                doc.body,
                json!({
                    "delete": {"_index": "test-token", "_id": "balance",
                               "version_type": "external", "version": (2u64 << 32) | 1}
                }),
            ]
        );
    }
}
