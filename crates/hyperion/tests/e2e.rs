//! End-to-end pipeline test: a mock SHIP websocket server streams one
//! synthetic block (token transfer with notifications, contract rows,
//! permission delta, signed block header) to the real indexer, which writes
//! to a mock Elasticsearch capturing `_bulk` bodies. Assertions run against
//! the exact documents that would have been indexed.

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
    let mut buf = Vec::new();
    push_varuint32(&mut buf, 1); // get_blocks_result_v0
    push_block_position(&mut buf, 100, 0x0a); // head
    push_block_position(&mut buf, 90, 0x0b); // lib
    buf.push(1);
    push_block_position(&mut buf, 42, 0xab); // this_block
    buf.push(1);
    push_block_position(&mut buf, 41, 0xac); // prev_block
                                             // Field order per the SHIP ABI: block, traces, deltas.
    buf.push(1);
    push_bytes(&mut buf, &block_payload());
    buf.push(1);
    push_bytes(&mut buf, &traces_payload());
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

type BulkLog = Arc<Mutex<Vec<String>>>;

async fn mock_es_handler(
    State(bulk_log): State<BulkLog>,
    method: Method,
    uri: Uri,
    body: Bytes,
) -> impl IntoResponse {
    let path = uri.path();
    match (method.as_str(), path) {
        ("GET", "/") => (
            StatusCode::OK,
            json!({"version": {"number": "8.99.0-mock"}}).to_string(),
        ),
        ("POST", "/v1/chain/get_abi") => {
            let req: Value = serde_json::from_slice(&body).unwrap_or_default();
            let abi = if req["account_name"] == "eosio.token" {
                json!({"account_name": "eosio.token", "abi": token_abi_json()})
            } else {
                json!({"account_name": req["account_name"], "abi": null})
            };
            (StatusCode::OK, abi.to_string())
        }
        // PulseVM JSON-RPC 2.0 chain API (POSTed to the base URL).
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
        ("POST", "/_bulk") => {
            bulk_log
                .lock()
                .unwrap()
                .push(String::from_utf8_lossy(&body).into_owned());
            (
                StatusCode::OK,
                json!({"errors": false, "items": []}).to_string(),
            )
        }
        _ => (StatusCode::OK, "{}".to_string()),
    }
}

async fn run_mock_ship(listener: tokio::net::TcpListener) {
    let (stream, _) = listener.accept().await.expect("ship accept");
    let mut ws = tokio_tungstenite::accept_async(stream)
        .await
        .expect("ship handshake");
    // 1. announce ABI
    ws.send(Message::Text("{\"version\": \"eosio::abi/1.1\"}".into()))
        .await
        .unwrap();
    // 2. serve requests
    while let Some(Ok(msg)) = ws.next().await {
        let Message::Binary(data) = msg else { continue };
        match data.first() {
            Some(0) => ws
                .send(Message::Binary(status_result().into()))
                .await
                .unwrap(),
            Some(1) => {
                // get_blocks_request_v0: verify requested range
                let start = u32::from_le_bytes(data[1..5].try_into().unwrap());
                let end = u32::from_le_bytes(data[5..9].try_into().unwrap());
                assert_eq!(start, 42);
                assert_eq!(end, 43);
                ws.send(Message::Binary(blocks_result().into()))
                    .await
                    .unwrap();
            }
            _ => {} // acks
        }
    }
}

// ---------------------------------------------------------------------------

/// Spin up the mock SHIP + mock ES/chain-API servers, run the real indexer
/// over one block, and return the (index, doc) pairs captured from `_bulk`.
async fn run_pipeline(chain_api: &str) -> Vec<(String, Value)> {
    let es_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let es_addr = es_listener.local_addr().unwrap();
    let bulk_log: BulkLog = Arc::new(Mutex::new(Vec::new()));
    let app = Router::new()
        .fallback(mock_es_handler)
        .with_state(bulk_log.clone());
    tokio::spawn(async move { axum::serve(es_listener, app).await.unwrap() });

    let ship_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ship_addr = ship_listener.local_addr().unwrap();
    tokio::spawn(run_mock_ship(ship_listener));

    let config: Config = toml::from_str(&format!(
        r#"
        [chain]
        name = "test"
        http = "http://{es_addr}"
        ship = "ws://{ship_addr}"
        api = "{chain_api}"

        [indexer]
        start_block = 42
        stop_block = 42

        [elasticsearch]
        url = "http://{es_addr}"
        "#
    ))
    .unwrap();

    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        hyperion::indexer::run(config),
    )
    .await
    .expect("indexer timed out")
    .expect("indexer failed");

    // Parse every doc out of the captured bulk bodies: action lines are
    // `{"index": {...}}\n{doc}` pairs.
    let mut docs: Vec<(String, Value)> = Vec::new();
    for body in bulk_log.lock().unwrap().iter() {
        let mut lines = body.lines();
        while let Some(meta) = lines.next() {
            let meta: Value = serde_json::from_str(meta).unwrap();
            if let Some(op) = meta.get("index") {
                let doc: Value = serde_json::from_str(lines.next().unwrap()).unwrap();
                docs.push((op["_index"].as_str().unwrap().to_string(), doc));
            }
        }
    }
    docs
}

/// The chain API is only consulted for ABI cache misses; with `pulsevm` the
/// same fetch goes through JSON-RPC (`pulsevm.getABI`). Action data decoding
/// proves the round-trip worked.
#[tokio::test]
async fn indexes_with_pulsevm_chain_api() {
    let docs = run_pipeline("pulsevm").await;
    let action = docs
        .iter()
        .find(|(i, _)| i == "test-action")
        .map(|(_, d)| d)
        .expect("action doc missing");
    assert_eq!(action["act"]["data"]["quantity"], "1.0000 EOS");
    assert_eq!(action["act"]["data"]["from"], "alice");
    let delta = docs
        .iter()
        .find(|(i, _)| i == "test-delta")
        .map(|(_, d)| d)
        .unwrap();
    assert_eq!(delta["data"]["balance"], "12.3456 EOS");
}

#[tokio::test]
async fn indexes_a_block_end_to_end() {
    let docs = run_pipeline("antelope").await;

    // Exactly one action doc: the three notification traces collapse.
    let actions: Vec<&Value> = docs
        .iter()
        .filter(|(i, _)| i == "test-action")
        .map(|(_, d)| d)
        .collect();
    assert_eq!(
        actions.len(),
        1,
        "notifications should collapse into one doc"
    );
    let action = actions[0];
    assert_eq!(action["global_sequence"], 777);
    assert_eq!(action["block_num"], 42);
    assert_eq!(action["trx_id"], "ee".repeat(32));
    assert_eq!(action["producer"], "producer1");
    assert_eq!(action["act"]["account"], "eosio.token");
    assert_eq!(action["act"]["name"], "transfer");
    assert_eq!(action["act"]["data"]["from"], "alice");
    assert_eq!(action["act"]["data"]["quantity"], "1.0000 EOS");
    assert_eq!(action["notified"], json!(["eosio.token", "alice", "bob"]));
    assert_eq!(action["receipts"].as_array().unwrap().len(), 3);
    assert!(action["signatures"][0]
        .as_str()
        .unwrap()
        .starts_with("SIG_K1_"));

    let block = docs
        .iter()
        .find(|(i, _)| i == "test-block")
        .map(|(_, d)| d)
        .unwrap();
    assert_eq!(block["block_num"], 42);
    assert_eq!(block["producer"], "producer1");
    assert_eq!(block["trx_count"], 1);
    // slot 1000 => 2000-01-01T00:08:20.000
    assert_eq!(block["@timestamp"], "2000-01-01T00:08:20.000");

    let delta = docs
        .iter()
        .find(|(i, _)| i == "test-delta")
        .map(|(_, d)| d)
        .unwrap();
    assert_eq!(delta["code"], "eosio.token");
    assert_eq!(delta["table"], "accounts");
    assert_eq!(delta["data"]["balance"], "12.3456 EOS");

    let token = docs
        .iter()
        .find(|(i, _)| i == "test-token")
        .map(|(_, d)| d)
        .unwrap();
    assert_eq!(token["scope"], "alice");
    assert_eq!(token["symbol"], "EOS");
    assert_eq!(token["amount"], 12.3456);

    let perm = docs
        .iter()
        .find(|(i, _)| i == "test-perm")
        .map(|(_, d)| d)
        .unwrap();
    assert_eq!(perm["owner"], "bob");
    assert_eq!(perm["name"], "active");
    assert_eq!(
        perm["keys"],
        json!(["PUB_K1_11111111111111111111111111111111149Mr2R"])
    );
}
