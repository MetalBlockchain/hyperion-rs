//! End-to-end pipeline test: a mock SHIP websocket server streams one
//! synthetic block (token transfer with notifications, contract rows,
//! permission delta, signed block header) to the real indexer, which writes
//! to a mock ClickHouse capturing `INSERT ... FORMAT TabSeparated` bodies.
//! Assertions run against the exact rows that would have been inserted,
//! reading fields by position (every table's column order is fixed by
//! `clickhouse::schema::create_tables_sql`, with `block_num` always first).

use antelope::Name;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{Method, StatusCode, Uri};
use axum::response::IntoResponse;
use axum::Router;
use futures_util::{SinkExt, StreamExt};
use hyperion::config::Config;
use serde_json::{json, Value};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use tokio_tungstenite::tungstenite::Message;

// ---------------------------------------------------------------------------
// Binary fixture encoders
// ---------------------------------------------------------------------------

fn push_varuint32(buf: &mut Vec<u8>, mut v: u32) {
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            buf.push(byte);
            break;
        }
        buf.push(byte | 0x80);
    }
}

fn push_name(buf: &mut Vec<u8>, name: &str) {
    buf.extend_from_slice(&Name::from_str(name).unwrap().0.to_le_bytes());
}

fn push_bytes(buf: &mut Vec<u8>, data: &[u8]) {
    push_varuint32(buf, data.len() as u32);
    buf.extend_from_slice(data);
}

fn push_block_position(buf: &mut Vec<u8>, num: u32, fill: u8) {
    buf.extend_from_slice(&num.to_le_bytes());
    buf.extend_from_slice(&[fill; 32]);
}

fn transfer_data() -> Vec<u8> {
    // transfer{from: alice, to: bob, quantity: 1.0000 EOS, memo: "hi"}
    let mut data = Vec::new();
    push_name(&mut data, "alice");
    push_name(&mut data, "bob");
    data.extend_from_slice(&10000i64.to_le_bytes());
    data.extend_from_slice(&"4,EOS".parse::<antelope::Symbol>().unwrap().0.to_le_bytes());
    push_bytes(&mut data, b"hi");
    data
}

/// One action_trace_v1 for the transfer, delivered to `receiver`.
fn action_trace(buf: &mut Vec<u8>, ordinal: u32, receiver: &str, global_sequence: u64) {
    push_varuint32(buf, 1); // action_trace_v1
    push_varuint32(buf, ordinal);
    push_varuint32(buf, 0); // creator_action_ordinal
    buf.push(1); // receipt present
    push_varuint32(buf, 0); // action_receipt_v0
    push_name(buf, receiver);
    buf.extend_from_slice(&[0xdd; 32]); // act_digest (same for all notifications)
    buf.extend_from_slice(&global_sequence.to_le_bytes());
    buf.extend_from_slice(&1u64.to_le_bytes()); // recv_sequence
    push_varuint32(buf, 1); // auth_sequence
    push_name(buf, "alice");
    buf.extend_from_slice(&7u64.to_le_bytes());
    push_varuint32(buf, 1); // code_sequence
    push_varuint32(buf, 1); // abi_sequence
    push_name(buf, receiver); // action_trace receiver
                              // act
    push_name(buf, "eosio.token");
    push_name(buf, "transfer");
    push_varuint32(buf, 1);
    push_name(buf, "alice");
    push_name(buf, "active");
    push_bytes(buf, &transfer_data());
    buf.push(0); // context_free
    buf.extend_from_slice(&50i64.to_le_bytes()); // elapsed
    push_bytes(buf, b""); // console
    push_varuint32(buf, 0); // account_ram_deltas
    buf.push(0); // except
    buf.push(0); // error_code
    push_bytes(buf, &[]); // return_value
}

fn traces_payload() -> Vec<u8> {
    let mut buf = Vec::new();
    push_varuint32(&mut buf, 1); // one transaction trace
    push_varuint32(&mut buf, 0); // transaction_trace_v0
    buf.extend_from_slice(&[0xee; 32]); // trx id
    buf.push(0); // executed
    buf.extend_from_slice(&150u32.to_le_bytes());
    push_varuint32(&mut buf, 12);
    buf.extend_from_slice(&2000i64.to_le_bytes());
    buf.extend_from_slice(&96u64.to_le_bytes());
    buf.push(0); // scheduled
    push_varuint32(&mut buf, 3); // primary + two notifications
    action_trace(&mut buf, 1, "eosio.token", 777);
    action_trace(&mut buf, 2, "alice", 778);
    action_trace(&mut buf, 3, "bob", 779);
    buf.push(0); // account_ram_delta
    buf.push(0); // except
    buf.push(0); // error_code
    buf.push(0); // failed_dtrx_trace
    buf.push(1); // partial present
    push_varuint32(&mut buf, 0); // partial_transaction_v0
    buf.extend_from_slice(&1700000000u32.to_le_bytes());
    buf.extend_from_slice(&12u16.to_le_bytes());
    buf.extend_from_slice(&34u32.to_le_bytes());
    push_varuint32(&mut buf, 0);
    buf.push(0);
    push_varuint32(&mut buf, 0);
    push_varuint32(&mut buf, 0); // transaction_extensions
    push_varuint32(&mut buf, 1); // one signature
    buf.push(0);
    buf.extend_from_slice(&[0x11; 65]);
    push_varuint32(&mut buf, 0); // context_free_data
    buf
}

fn deltas_payload() -> Vec<u8> {
    let mut buf = Vec::new();
    push_varuint32(&mut buf, 2); // two table deltas

    // contract_row: alice's eosio.token balance
    push_varuint32(&mut buf, 0); // table_delta_v0
    push_bytes(&mut buf, b"contract_row");
    push_varuint32(&mut buf, 1);
    buf.push(1); // present
    let mut row = Vec::new();
    push_varuint32(&mut row, 0); // contract_row_v0
    push_name(&mut row, "eosio.token");
    push_name(&mut row, "alice");
    push_name(&mut row, "accounts");
    row.extend_from_slice(&5u64.to_le_bytes());
    push_name(&mut row, "alice");
    let mut balance = Vec::new();
    balance.extend_from_slice(&123456i64.to_le_bytes());
    balance.extend_from_slice(&"4,EOS".parse::<antelope::Symbol>().unwrap().0.to_le_bytes());
    push_bytes(&mut row, &balance);
    push_bytes(&mut buf, &row);

    // permission: bob@active with one key
    push_varuint32(&mut buf, 0); // table_delta_v0
    push_bytes(&mut buf, b"permission");
    push_varuint32(&mut buf, 1);
    buf.push(1); // present
    let mut row = Vec::new();
    push_varuint32(&mut row, 0); // permission_v0
    push_name(&mut row, "bob"); // owner
    push_name(&mut row, "active"); // name
    push_name(&mut row, "owner"); // parent
    row.extend_from_slice(&1_600_000_000_000_000i64.to_le_bytes()); // last_updated
    row.extend_from_slice(&1u32.to_le_bytes()); // threshold
    push_varuint32(&mut row, 1); // one key
    row.push(0); // K1
    row.extend_from_slice(&[0u8; 33]); // the all-zeros key
    row.extend_from_slice(&1u16.to_le_bytes()); // weight
    push_varuint32(&mut row, 0); // accounts
    push_varuint32(&mut row, 0); // waits
    push_bytes(&mut buf, &row);

    buf
}

fn block_payload() -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&1000u32.to_le_bytes()); // timestamp slot
    push_name(&mut buf, "producer1");
    buf.extend_from_slice(&0u16.to_le_bytes()); // confirmed
    buf.extend_from_slice(&[0xac; 32]); // previous
    buf.extend_from_slice(&[0x01; 32]); // transaction_mroot
    buf.extend_from_slice(&[0x02; 32]); // action_mroot
    buf.extend_from_slice(&7u32.to_le_bytes()); // schedule_version
    buf.push(0); // new_producers
    push_varuint32(&mut buf, 0); // header_extensions
    buf.push(0); // producer_signature: K1
    buf.extend_from_slice(&[0x22; 65]);
    push_varuint32(&mut buf, 1); // one transaction receipt
    buf.push(0); // status
    buf.extend_from_slice(&150u32.to_le_bytes());
    push_varuint32(&mut buf, 12);
    push_varuint32(&mut buf, 0); // trx variant: transaction_id
    buf.extend_from_slice(&[0xee; 32]);
    buf
}

fn status_result() -> Vec<u8> {
    let mut buf = Vec::new();
    push_varuint32(&mut buf, 0); // get_status_result_v0
    push_block_position(&mut buf, 100, 0x0a); // head
    push_block_position(&mut buf, 90, 0x0b); // last_irreversible
    buf.extend_from_slice(&0u32.to_le_bytes()); // trace_begin
    buf.extend_from_slice(&101u32.to_le_bytes()); // trace_end
    buf.extend_from_slice(&0u32.to_le_bytes()); // chain_state_begin
    buf.extend_from_slice(&101u32.to_le_bytes()); // chain_state_end
    buf.extend_from_slice(&[0xcc; 32]); // chain_id
    buf
}

fn blocks_result() -> Vec<u8> {
    blocks_result_at(42, 1)
}

fn blocks_result_at(block_num: u32, transactions: u32) -> Vec<u8> {
    let mut buf = Vec::new();
    push_varuint32(&mut buf, 1); // get_blocks_result_v0
    push_block_position(&mut buf, 100, 0x0a); // head
    push_block_position(&mut buf, 90, 0x0b); // lib
    buf.push(1);
    push_block_position(&mut buf, block_num, 0xab); // this_block
    buf.push(1);
    push_block_position(&mut buf, block_num - 1, 0xac); // prev_block
                                                        // Field order per the SHIP ABI: block, traces, deltas.
    buf.push(1);
    push_bytes(&mut buf, &block_payload());
    buf.push(1);
    let trace = traces_payload();
    let mut traces = Vec::new();
    push_varuint32(&mut traces, transactions);
    for _ in 0..transactions {
        traces.extend_from_slice(&trace[1..]);
    }
    push_bytes(&mut buf, &traces);
    buf.push(1);
    push_bytes(&mut buf, &deltas_payload());
    buf
}

// ---------------------------------------------------------------------------
// Mock servers
// ---------------------------------------------------------------------------

fn token_abi_json() -> Value {
    json!({
        "version": "eosio::abi/1.1",
        "types": [],
        "structs": [
            {"name": "transfer", "base": "", "fields": [
                {"name": "from", "type": "name"},
                {"name": "to", "type": "name"},
                {"name": "quantity", "type": "asset"},
                {"name": "memo", "type": "string"}
            ]},
            {"name": "account", "base": "", "fields": [
                {"name": "balance", "type": "asset"}
            ]}
        ],
        "actions": [{"name": "transfer", "type": "transfer", "ricardian_contract": ""}],
        "tables": [{"name": "accounts", "index_type": "i64", "key_names": [], "key_types": [], "type": "account"}],
        "variants": []
    })
}

/// `(table, tab_separated_row)` pairs captured from every insert this test
/// run performed, in submission order per table (ClickHouse's own HTTP
/// `query` parameter carries `INSERT INTO <table> FORMAT TabSeparated`; the
/// mock below reads the table name out of that string).
type InsertLog = Arc<Mutex<Vec<(String, String)>>>;

fn insert_table_name(query: &str) -> Option<&str> {
    query.strip_prefix("INSERT INTO ")?.split(' ').next()
}

async fn mock_chain_and_clickhouse(
    State(insert_log): State<InsertLog>,
    method: Method,
    uri: Uri,
    body: Bytes,
) -> impl IntoResponse {
    let path = uri.path();
    // Minimal `application/x-www-form-urlencoded` decode of just the `query`
    // parameter - avoids pulling in a URL-encoding crate for one mock.
    let query_param: Option<String> = uri.query().and_then(|q| {
        q.split('&').find_map(|pair| {
            let (key, value) = pair.split_once('=')?;
            (key == "query").then(|| {
                value
                    .replace('+', " ")
                    .split('%')
                    .enumerate()
                    .map(|(i, part)| {
                        if i == 0 {
                            part.to_string()
                        } else if part.len() >= 2 {
                            let byte = u8::from_str_radix(&part[..2], 16).unwrap_or(b'?');
                            format!("{}{}", byte as char, &part[2..])
                        } else {
                            part.to_string()
                        }
                    })
                    .collect::<String>()
            })
        })
    });
    match (method.as_str(), path) {
        ("GET", "/ping") => (StatusCode::OK, "Ok.".to_string()),
        ("POST", "/v1/chain/get_abi") => {
            let req: Value = serde_json::from_slice(&body).unwrap_or_default();
            let abi = if req["account_name"] == "eosio.token" {
                json!({"account_name": "eosio.token", "abi": token_abi_json()})
            } else {
                json!({"account_name": req["account_name"], "abi": null})
            };
            (StatusCode::OK, abi.to_string())
        }
        // ClickHouse's HTTP interface: every non-insert statement (ping
        // aside) is a GET with the SQL in `?query=`; inserts are POSTs with
        // the SQL in `?query=` and the TabSeparated data as the body.
        ("GET", "/") if query_param.is_some() => (StatusCode::OK, String::new()),
        ("POST", "/")
            if query_param
                .as_deref()
                .is_some_and(|q| q.starts_with("INSERT INTO")) =>
        {
            let query = query_param.unwrap();
            let table = insert_table_name(&query)
                .expect("insert query names a table")
                .to_string();
            let data = String::from_utf8_lossy(&body).into_owned();
            let mut log = insert_log.lock().unwrap();
            for line in data.lines() {
                log.push((table.clone(), line.to_string()));
            }
            (StatusCode::OK, String::new())
        }
        // PulseVM JSON-RPC 2.0 chain API (POSTed to the base URL, same as
        // ClickHouse's own POST / - distinguished by JSON vs `?query=`).
        ("POST", "/") => {
            let req: Value = serde_json::from_slice(&body).unwrap_or_default();
            let id = req["id"].clone();
            let response = match req["method"].as_str() {
                Some("pulsevm.getInfo") => json!({
                    "jsonrpc": "2.0", "id": id,
                    "result": {
                        "head_block_num": 100,
                        "last_irreversible_block_num": 90,
                        "chain_id": "cc".repeat(32),
                    }
                }),
                Some("pulsevm.getABI") => {
                    if req["params"]["account_name"] == "eosio.token" {
                        json!({"jsonrpc": "2.0", "id": id, "result": token_abi_json()})
                    } else {
                        json!({
                            "jsonrpc": "2.0", "id": id,
                            "error": {"code": 400, "message": "unknown key"}
                        })
                    }
                }
                _ => json!({
                    "jsonrpc": "2.0", "id": id,
                    "error": {"code": -32601, "message": "Method not found"}
                }),
            };
            (StatusCode::OK, response.to_string())
        }
        _ => (StatusCode::OK, "{}".to_string()),
    }
}

async fn run_mock_ship(listener: tokio::net::TcpListener, transactions: u32) {
    let (stream, _) = listener.accept().await.expect("ship accept");
    let mut ws = tokio_tungstenite::accept_async(stream)
        .await
        .expect("ship handshake");
    // 1. announce ABI
    ws.send(Message::Text("{\"version\": \"eosio::abi/1.1\"}".into()))
        .await
        .unwrap();
    // 2. serve requests
    let mut next_block = 42;
    let mut end_block = 42;
    while let Some(Ok(msg)) = ws.next().await {
        let Message::Binary(data) = msg else { continue };
        let credit = match data.first() {
            Some(0) => {
                ws.send(Message::Binary(status_result().into()))
                    .await
                    .unwrap();
                0
            }
            Some(1) => {
                // get_blocks_request_v0: verify requested range
                let start = u32::from_le_bytes(data[1..5].try_into().unwrap());
                let end = u32::from_le_bytes(data[5..9].try_into().unwrap());
                assert_eq!(start, 42);
                next_block = start;
                end_block = end;
                u32::from_le_bytes(data[9..13].try_into().unwrap())
            }
            Some(2) => u32::from_le_bytes(data[1..5].try_into().unwrap()),
            _ => 0,
        };
        for _ in 0..credit.min(end_block - next_block) {
            let payload = if next_block == 42 && transactions == 1 {
                blocks_result()
            } else {
                blocks_result_at(next_block, transactions)
            };
            if ws.send(Message::Binary(payload.into())).await.is_err() {
                return;
            }
            next_block += 1;
        }
    }
}

// ---------------------------------------------------------------------------

/// Spin up the mock SHIP + mock ClickHouse/chain-API servers, run the real
/// indexer over one block, and return the `(table, row)` pairs that would
/// have been inserted.
async fn run_pipeline(chain_api: &str) -> Vec<(String, String)> {
    run_pipeline_options(chain_api, PipelineOptions::default())
        .await
        .unwrap()
}

struct PipelineOptions {
    blocks: u32,
    transactions: u32,
    batch_size: usize,
    insert_delay_ms: u64,
    fail_insert: bool,
    decode_workers: usize,
    writer_concurrency: usize,
}

impl Default for PipelineOptions {
    fn default() -> Self {
        Self {
            blocks: 1,
            transactions: 1,
            batch_size: 2000,
            insert_delay_ms: 0,
            fail_insert: false,
            decode_workers: 2,
            writer_concurrency: 4,
        }
    }
}

async fn run_pipeline_options(
    chain_api: &str,
    options: PipelineOptions,
) -> anyhow::Result<Vec<(String, String)>> {
    let ch_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ch_addr = ch_listener.local_addr().unwrap();
    let insert_log: InsertLog = Arc::new(Mutex::new(Vec::new()));
    let app = Router::new()
        .fallback(
            move |state: State<InsertLog>, method: Method, uri: Uri, body: Bytes| async move {
                let is_insert = method == Method::POST
                    && uri
                        .query()
                        .is_some_and(|q| q.contains("INSERT+INTO") || q.contains("INSERT%20INTO"));
                if is_insert {
                    tokio::time::sleep(std::time::Duration::from_millis(options.insert_delay_ms))
                        .await;
                    if options.fail_insert {
                        return (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "injected failure".to_string(),
                        )
                            .into_response();
                    }
                }
                mock_chain_and_clickhouse(state, method, uri, body)
                    .await
                    .into_response()
            },
        )
        .with_state(insert_log.clone())
        .layer(axum::extract::DefaultBodyLimit::disable());
    let mut servers = tokio::task::JoinSet::new();
    servers.spawn(async move { axum::serve(ch_listener, app).await.unwrap() });

    let ship_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ship_addr = ship_listener.local_addr().unwrap();
    servers.spawn(run_mock_ship(ship_listener, options.transactions));

    let stop_block = 41 + options.blocks;
    let batch_size = options.batch_size;
    let decode_workers = options.decode_workers;
    let writer_concurrency = options.writer_concurrency;

    let config: Config = toml::from_str(&format!(
        r#"
        [chain]
        name = "test"
        http = "http://{ch_addr}"
        ship = "ws://{ship_addr}"
        api = "{chain_api}"

        [indexer]
        start_block = 42
        stop_block = {stop_block}
        batch_size = {batch_size}
        decode_workers = {decode_workers}
        writer_concurrency = {writer_concurrency}

        [clickhouse]
        url = "http://{ch_addr}"
        "#
    ))
    .unwrap();

    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        hyperion::indexer::run(config),
    )
    .await
    .expect("indexer timed out")?;

    let rows = insert_log.lock().unwrap().clone();
    Ok(rows)
}

/// Split a captured TabSeparated row into its raw field strings. Array/tuple
/// columns (`writer::array_to_string`/`tuple_array_to_string`) contain no
/// literal tabs - their contents are escaped per-element - so a plain split
/// on `\t` is exact.
fn fields(row: &str) -> Vec<&str> {
    row.split('\t').collect()
}

#[tokio::test]
async fn drains_ordered_batches_with_a_slow_writer() {
    // Force a flush on every single row (there is no ClickHouse equivalent
    // of the old byte-size threshold - see `indexer::process_blocks`), then
    // confirm a large threshold still flushes everything via the
    // end-of-stream drain. `i64::MAX` is TOML's own integer ceiling.
    for batch_size in [1, i64::MAX as usize] {
        let docs = run_pipeline_options(
            "antelope",
            PipelineOptions {
                blocks: 12,
                batch_size,
                insert_delay_ms: 2,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        for table in ["block", "action", "delta", "token", "perm"] {
            let numbers: Vec<_> = docs
                .iter()
                .filter(|(t, _)| t == table)
                .map(|(_, row)| fields(row)[0].parse::<u64>().unwrap())
                .collect();
            assert_eq!(numbers, (42..54).collect::<Vec<_>>(), "{table} write order");
        }
    }
}

#[tokio::test]
async fn parallel_decoding_matches_inline_documents() {
    let expected = run_pipeline_options(
        "antelope",
        PipelineOptions {
            blocks: 12,
            transactions: 4,
            decode_workers: 0,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    for workers in [1, 2, 4] {
        let actual = run_pipeline_options(
            "antelope",
            PipelineOptions {
                blocks: 12,
                transactions: 4,
                decode_workers: workers,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(
            actual, expected,
            "documents differ with {workers} decoder workers"
        );
    }
}

#[tokio::test]
async fn propagates_insert_failures() {
    let error = run_pipeline_options(
        "antelope",
        PipelineOptions {
            blocks: 12,
            batch_size: 1,
            fail_insert: true,
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert!(
        error.to_string().contains("clickhouse insert failed"),
        "{error}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "synthetic throughput benchmark; run explicitly in release mode"]
async fn benchmark_pipeline() {
    let blocks = 4000;
    for writer_concurrency in [1, 4, 8] {
        for delay in [0, 10, 50, 100] {
            let start = std::time::Instant::now();
            let docs = run_pipeline_options(
                "antelope",
                PipelineOptions {
                    blocks,
                    transactions: 16,
                    insert_delay_ms: delay,
                    decode_workers: 2,
                    writer_concurrency,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            let elapsed = start.elapsed();
            assert_eq!(docs.len(), blocks as usize * 20);
            eprintln!(
            "writer_concurrency={writer_concurrency}, insert_delay_ms={delay}: {blocks} blocks, {} docs in {:.3}s ({:.0} blocks/s)",
            docs.len(),
            elapsed.as_secs_f64(),
            blocks as f64 / elapsed.as_secs_f64()
        );
        }
    }
}

/// The chain API is only consulted for ABI cache misses; with `pulsevm` the
/// same fetch goes through JSON-RPC (`pulsevm.getABI`). Action data decoding
/// proves the round-trip worked.
#[tokio::test]
async fn indexes_with_pulsevm_chain_api() {
    let docs = run_pipeline("pulsevm").await;
    let action = docs
        .iter()
        .find(|(t, _)| t == "action")
        .map(|(_, row)| fields(row))
        .expect("action row missing");
    let act_data: Value = serde_json::from_str(action[9]).unwrap();
    assert_eq!(act_data["quantity"], "1.0000 EOS");
    assert_eq!(act_data["from"], "alice");

    let delta = docs
        .iter()
        .find(|(t, _)| t == "delta")
        .map(|(_, row)| fields(row))
        .unwrap();
    let delta_data: Value = serde_json::from_str(delta[9]).unwrap();
    assert_eq!(delta_data["balance"], "12.3456 EOS");
}

#[tokio::test]
async fn indexes_a_block_end_to_end() {
    let docs = run_pipeline("antelope").await;

    // Exactly one action row: the three notification traces collapse.
    let actions: Vec<Vec<&str>> = docs
        .iter()
        .filter(|(t, _)| t == "action")
        .map(|(_, row)| fields(row))
        .collect();
    assert_eq!(
        actions.len(),
        1,
        "notifications should collapse into one row"
    );
    let action = &actions[0];
    assert_eq!(action[0], "42", "block_num"); // block_num
    assert_eq!(action[1], "777", "global_sequence");
    assert_eq!(action[4], "ee".repeat(32), "trx_id");
    assert_eq!(action[5], "producer1", "producer");
    assert_eq!(action[6], "eosio.token", "act_account");
    assert_eq!(action[7], "transfer", "act_name");
    let act_data: Value = serde_json::from_str(action[9]).unwrap();
    assert_eq!(act_data["from"], "alice");
    assert_eq!(act_data["quantity"], "1.0000 EOS");
    assert_eq!(action[18], "['eosio.token','alice','bob']", "notified");
    // receipts: one Tuple(receiver, global_sequence) per notification.
    assert_eq!(
        action[19].matches("),(").count() + 1,
        3,
        "receipts: {}",
        action[19]
    );
    assert!(action[24].contains("SIG_K1_"), "signatures: {}", action[24]);

    let block = docs
        .iter()
        .find(|(t, _)| t == "block")
        .map(|(_, row)| fields(row))
        .unwrap();
    assert_eq!(block[0], "42", "block_num");
    assert_eq!(block[4], "producer1", "producer");
    assert_eq!(block[6], "1", "trx_count");
    // slot 1000 => 2000-01-01T00:08:20.000. The writer passes
    // `processor`'s ISO-8601 string straight through as the TSV field (see
    // `writer::block_row`); a real ClickHouse server stores it as `DateTime`
    // (whole-second resolution, its own text format on read-back) rather
    // than preserving this exact string - this mock only captures what was
    // sent, not what ClickHouse would make of it.
    assert_eq!(block[1], "2000-01-01T00:08:20.000", "timestamp");

    let delta = docs
        .iter()
        .find(|(t, _)| t == "delta")
        .map(|(_, row)| fields(row))
        .unwrap();
    assert_eq!(delta[3], "eosio.token", "code");
    assert_eq!(delta[5], "accounts", "table");
    let delta_data: Value = serde_json::from_str(delta[9]).unwrap();
    assert_eq!(delta_data["balance"], "12.3456 EOS");

    let token = docs
        .iter()
        .find(|(t, _)| t == "token")
        .map(|(_, row)| fields(row))
        .unwrap();
    assert_eq!(token[2], "alice", "scope");
    assert_eq!(token[3], "EOS", "symbol");
    assert_eq!(token[5], "12.3456", "amount");

    let perm = docs
        .iter()
        .find(|(t, _)| t == "perm")
        .map(|(_, row)| fields(row))
        .unwrap();
    assert_eq!(perm[1], "bob", "owner");
    assert_eq!(perm[2], "active", "name");
    assert_eq!(
        perm[5], "['PUB_K1_11111111111111111111111111111111149Mr2R']",
        "keys"
    );
}
