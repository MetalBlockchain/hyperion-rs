//! The indexing pipeline: SHIP reader → parallel raw decoders → ordered
//! processor → ClickHouse batch writer. Hyperion uses RabbitMQ between
//! these stages; here they are
//! in-process tasks connected by bounded channels, with SHIP's own
//! credit-based flow control providing end-to-end backpressure.

use crate::abis::AbiCache;
use crate::clickhouse::{ClickHouse, ClickHouseBatch};
use crate::config::Config;
use crate::processor::{DecodedBlock, Processor};
use anyhow::{Context, Result};
use ship::{GetBlocksRequest, GetBlocksResult, ShipClient, ShipResult};
use std::collections::BTreeMap;
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
        config.indexer.flush_interval_ms > 0,
        "flush_interval_ms must be positive"
    );
    let ck = ClickHouse::new(
        config.clickhouse.url.clone(),
        config.clickhouse.user.clone(),
        config.clickhouse.pass.clone(),
    );
    ck.ping().await.context("cannot reach clickhouse")?;
    ck.create_all()
        .await
        .context("cannot create clickhouse tables")?;
    tracing::info!(url = %config.clickhouse.url, "connected to clickhouse");

    let start_block = resolve_start_block(&config, &ck).await?;
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

    let (batch_tx, batch_rx) = mpsc::channel(2);
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
    let writer_concurrency = config.indexer.writer_concurrency;
    stages.spawn(async move { process_blocks(&config, input, batch_tx).await });
    stages.spawn(async move {
        crate::clickhouse::write_batches(&ck, batch_rx, writer_concurrency).await
    });
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

/// `ClickHouseBatch` keys rows by `doc.kind` directly (ClickHouse's table
/// names match the kinds exactly, see `clickhouse::schema::create_tables_sql`),
/// so there is no per-kind index name to look up, and it tracks a row count
/// only - unlike the old Elasticsearch bulk body there is no running
/// byte-size threshold to check.
async fn process_blocks(
    config: &Config,
    mut rx: BlockInput,
    tx: mpsc::Sender<ClickHouseBatch>,
) -> Result<()> {
    let system_account: antelope::Name = config
        .chain
        .system_account
        .parse()
        .map_err(|e| anyhow::anyhow!("bad chain.system_account: {e}"))?;
    let processor = Processor::new(&config.indexer.skip_actions, system_account);
    let mut abis = AbiCache::new(config.chain.client());
    let mut batch = ClickHouseBatch::new();
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
            let version = (u64::from(last_block) << 32) | seq as u64;
            batch.push(&doc, version)?;
        }
        blocks_since_report += 1;

        if batch.count > 0
            && (batch.count >= config.indexer.batch_size || last_flush.elapsed() >= flush_interval)
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

async fn resolve_start_block(config: &Config, ck: &ClickHouse) -> Result<u32> {
    if config.indexer.start_block > 0 {
        return Ok(config.indexer.start_block);
    }
    // The checkpoint only advances once batches complete contiguously (see
    // clickhouse::writer::write_batches), so it's safe to trust even though
    // writes themselves may complete out of order. Prefer it over the raw
    // aggregation below, which can't tell "durably confirmed" apart from
    // "visible but a lower block is still in flight or failed".
    if let Some(checkpoint) = ck.get_checkpoint().await? {
        tracing::info!(
            resume_from = checkpoint + 1,
            table = "progress",
            "resuming from last confirmed checkpoint"
        );
        return Ok(checkpoint + 1);
    }
    // Deployments predating the checkpoint row: fall back to the highest
    // indexed block; block table first, actions as fallback for deployments
    // that disable fetch_block.
    for kind in ["block", "action"] {
        if let Some(max) = ck.max_block_num(kind).await? {
            tracing::info!(
                resume_from = max + 1,
                table = kind,
                "resuming from last indexed block"
            );
            return Ok(max + 1);
        }
    }
    Ok(1)
}

#[cfg(test)]
mod tests {
    use super::*;

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

    /// Minimal mock of ClickHouse's HTTP interface: `POST /` is an insert
    /// (TabSeparated body; fails when the body starts with `fail`), `GET /`
    /// is every other statement run through `query()`/`execute()` (here,
    /// just the checkpoint `INSERT INTO progress ... VALUES (...)`, counted
    /// via the `query` parameter text since it's a GET, not a POST).
    fn mock_clickhouse(
        insert_requests: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        checkpoint_puts: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    ) -> axum::Router {
        use axum::extract::Query;
        use std::collections::HashMap;
        use std::sync::atomic::Ordering;

        axum::Router::new().route(
            "/",
            axum::routing::get(move |Query(params): Query<HashMap<String, String>>| {
                let checkpoint_puts = checkpoint_puts.clone();
                async move {
                    if params
                        .get("query")
                        .is_some_and(|q| q.contains("INSERT INTO progress"))
                    {
                        checkpoint_puts.fetch_add(1, Ordering::SeqCst);
                    }
                    ""
                }
            })
            .post(move |body: axum::body::Bytes| {
                let insert_requests = insert_requests.clone();
                async move {
                    insert_requests.fetch_add(1, Ordering::SeqCst);
                    if body.starts_with(b"fail") {
                        (
                            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                            "injected failure",
                        )
                    } else {
                        (axum::http::StatusCode::OK, "")
                    }
                }
            }),
        )
    }

    fn failing_batch(marker: &str, max_block: u32) -> ClickHouseBatch {
        let mut rows_by_table = std::collections::HashMap::new();
        rows_by_table.insert("action".to_string(), vec![vec![marker.to_string()]]);
        ClickHouseBatch {
            rows_by_table,
            max_block,
            count: 1,
        }
    }

    #[tokio::test]
    async fn clickhouse_writer_stops_before_submitting_queued_batches_after_failure() {
        use std::sync::{atomic::AtomicUsize, Arc};
        let insert_requests = Arc::new(AtomicUsize::new(0));
        let checkpoint_puts = Arc::new(AtomicUsize::new(0));
        let app = mock_clickhouse(insert_requests.clone(), checkpoint_puts);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut servers = tokio::task::JoinSet::new();
        servers.spawn(async move { axum::serve(listener, app).await.unwrap() });

        let ck = ClickHouse::new(format!("http://{addr}"), None, None);
        let (tx, rx) = mpsc::channel(2);
        tx.send(failing_batch("fail", 1)).await.unwrap();
        tx.send(failing_batch("fail", 2)).await.unwrap();
        drop(tx);
        // concurrency = 1 reproduces a fully-serial writer: the second batch
        // must never even be submitted once the first has failed.
        let error = crate::clickhouse::write_batches(&ck, rx, 1)
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("clickhouse insert failed"),
            "{error}"
        );
        assert_eq!(insert_requests.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn clickhouse_checkpoint_never_advances_past_a_failed_batch_even_if_a_later_one_lands_first(
    ) {
        use std::sync::{atomic::AtomicUsize, Arc};
        let insert_requests = Arc::new(AtomicUsize::new(0));
        let checkpoint_puts = Arc::new(AtomicUsize::new(0));
        let app = mock_clickhouse(insert_requests, checkpoint_puts.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut servers = tokio::task::JoinSet::new();
        servers.spawn(async move { axum::serve(listener, app).await.unwrap() });

        let ck = ClickHouse::new(format!("http://{addr}"), None, None);
        let (tx, rx) = mpsc::channel(2);
        // Ordinal 0 fails, ordinal 1 (a later block) would succeed on its own
        // - with concurrency > 1 both are in flight before either completes,
        // so ordinal 1 may well land on the wire first.
        tx.send(failing_batch("fail", 1)).await.unwrap();
        tx.send(failing_batch("ok", 2)).await.unwrap();
        drop(tx);
        let error = crate::clickhouse::write_batches(&ck, rx, 4)
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("clickhouse insert failed"),
            "{error}"
        );
        assert_eq!(
            checkpoint_puts.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "checkpoint must not advance while an earlier batch is unresolved or failed"
        );
    }
}
