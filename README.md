# hyperion-rs

A Rust implementation of [eosrio's Hyperion](https://github.com/eosrio/hyperion-history-api) —
a full-history solution for Antelope (EOSIO) blockchains. It consumes the
nodeos **state-history plugin (SHIP)** websocket feed, ABI-decodes action
traces and table deltas, indexes them into **ClickHouse**, and serves a
Hyperion-compatible **v2 REST API** (plus a nodeos v1 history compatibility
layer).

```
┌────────┐  SHIP ws   ┌─────────┐   channel   ┌───────────┐  INSERT   ┌───────────────┐
│ nodeos │ ─────────► │ reader  │ ──────────► │ processor │ ────────► │  ClickHouse   │
└────────┘  (binary)  └─────────┘ (backpress.)└───────────┘           └───────┬───────┘
     ▲                                          │ ABI cache                   │
     └──────────── /v1/chain/get_abi ───────────┘                     ┌───────┴───────┐
                                                                      │  API (axum)   │
                                                                      │ /v2/*  /v1/*  │
                                                                      └───────────────┘
```

## Workspace layout

| Crate | Purpose |
|---|---|
| `crates/antelope` | Core chain types (`name`/`symbol`/`asset` encodings, keys, timestamps), a binary reader for the Antelope serialization format, and an ABI-driven decoder (the `abieos` equivalent) that turns action data and table rows into JSON. Parses ABIs from both JSON and the packed binary form. |
| `crates/ship` | SHIP websocket client with hand-written codecs for the protocol: `get_status`/`get_blocks` requests, block results, transaction/action traces (v0/v1), table deltas, `contract_row`/`account`/`permission` state rows, and signed block headers. |
| `crates/hyperion` | The `hyperion` binary: the indexer pipeline and the API server, sharing one TOML config. |

## Design notes (vs. the Node.js original)

- **No RabbitMQ** — the reader → processor → batch-writer stages are
  in-process Tokio tasks connected by bounded channels; SHIP's own
  credit-based flow control provides end-to-end backpressure.
- **ClickHouse, not Elasticsearch** — every table is a `ReplacingMergeTree`
  keyed by a version derived from block position, so a replayed or
  out-of-order write is superseded rather than duplicated (see
  `clickhouse::schema`). `action`/`block`/`delta`/`abi` are append-only
  history; `perm`/`token` are current-state snapshots deduplicated by entity
  key (not by block), with deletes written as tombstone rows
  (`is_deleted = 1`) that every read query filters out explicitly.
- **ABI handling** — contract ABIs are tracked in block order from
  state-history `account` deltas (and the system account's `setabi` actions),
  so historical action data decodes with the ABI that was active at that
  block. On a cache miss (indexer started mid-chain) the current ABI is
  fetched from the chain API as a pragmatic fallback. Undecodable action data
  is indexed as `act.hex_data` instead of being dropped.
  Prepared ABI decoders are reused across actions and rows, and invalidated
  whenever an on-chain ABI update arrives.
- **Chain API dialects** — the chain HTTP API (used for `get_info` and the
  ABI fallback) speaks either the classic nodeos REST API (`api = "antelope"`)
  or PulseVM's JSON-RPC 2.0 API
  (`api = "pulsevm"`, methods `pulsevm.getInfo` / `pulsevm.getABI`, URL like
  `http://node:9650/ext/bc/<chainID>/rpc`). Set `system_account` to the
  chain's privileged account (`eosio` on Antelope, `pulse` on PulseVM).
- **Document model** — one doc per unique action (notification traces from
  `require_recipient` are collapsed into `notified` + `receipts`, keyed by
  `act_digest`), matching Hyperion's query semantics. Deterministic document
  IDs (`global_sequence`, `block_num`, …) make reindexing idempotent and let
  microforks overwrite stale docs.
- **Tables** — `action`, `block`, `delta`, `abi`, `perm` (current permissions,
  feeds `get_key_accounts`), `token` (current token balances, feeds
  `get_tokens`). Table names are fixed, not chain-prefixed, so one ClickHouse
  instance holds exactly one chain's history.

## Requirements

- Rust 1.85+ (2021 edition workspace)
- ClickHouse 23.8+ (tested against 24.10)
- A nodeos instance with the state-history plugin enabled
  (`--plugin eosio::state_history_plugin --trace-history --chain-state-history`)

## Running

```bash
cp config/example.toml config.toml   # then edit endpoints
cargo build --release

# fill ClickHouse from state history (resumes automatically)
./target/release/hyperion indexer -c config.toml

# serve the HTTP API
./target/release/hyperion api -c config.toml
```

### Docker

`docker-compose.yml` runs a single-node ClickHouse plus the indexer and
API (nodeos is not part of the stack — `config/docker.toml` defaults to a
state-history node on the docker host via `host.docker.internal`):

```bash
# edit config/docker.toml if nodeos is not on the docker host
docker compose up --build

curl http://localhost:7000/v2/health
```

ClickHouse data persists in the `chdata` volume; the API is published on
port 7000, ClickHouse's HTTP interface on 127.0.0.1:8123.

### Indexing throughput

Use a release build for indexing. Block processing and row serialization
overlap with ClickHouse inserts; up to `indexer.writer_concurrency` serialized
batches run concurrently (each table's rows for a batch in one insert).
Completions are confirmed in submission order so the resume checkpoint never
advances past a batch that's still in flight or failed, even though
ClickHouse may finish a later insert first.

Raw block headers, transaction traces, and table deltas are decoded on a
bounded CPU worker pool. `indexer.decode_workers` controls its concurrency;
the default is up to two workers, leaving one available CPU for the rest of
the pipeline. Set it to `0` to decode inline. Completed results are restored
to SHIP arrival order before ABI updates or document construction, including
when forks replay earlier block numbers. Running jobs and results waiting for
an earlier block share the worker limit; another two decoded blocks can wait
for the processor. Worker errors stop the pipeline. Already running CPU jobs
may finish after cancellation, but cannot index documents.

`indexer.batch_size` (default 2,000 rows) controls the batch target; there is
no ClickHouse equivalent of a byte-size threshold. It is checked after each
complete block, so a large block can exceed it. Partial batches are queued
after `flush_interval_ms` (default 500 ms); a busy writer can delay their
submission. Insert failures stop the pipeline rather than allowing later
batches to advance the indexed position: after a partial failure, restart
with an explicit `start_block` covering the failed batch.

Run the reproducible synthetic pipeline benchmark with:

```bash
cargo test --release -p hyperion --test e2e benchmark_pipeline -- --ignored --nocapture
```

It uses local mock SHIP, chain API, and ClickHouse servers (the mock only
captures what the client would have inserted; it isn't a ClickHouse server).
Its results measure processing and simulated write latency, not production
ClickHouse capacity. It compares 0, 1, 2, and 4 decoder workers; try the same
comparison with a representative block sample before raising the worker
count.

> The throughput numbers previously published here were measured against the
> Elasticsearch writer this branch replaces and are no longer representative
> - re-run the benchmark above against ClickHouse before relying on any
> specific figure.

## API endpoints

v2 (Hyperion-style, GET):

- `/v2/health`
- `/v2/history/get_actions?account=&filter=code:action&after=&before=&skip=&limit=&sort=&simple=`
- `/v2/history/get_transaction?id=`
- `/v2/history/get_deltas?code=&scope=&table=&payer=&present=`
- `/v2/history/get_abi_snapshot?contract=&block=`
- `/v2/history/get_created_accounts?account=`
- `/v2/history/get_creator?account=`
- `/v2/state/get_key_accounts?public_key=` (accepts `PUB_K1_...` or legacy `EOS...`)
- `/v2/state/get_tokens?account=`
- `/v2/state/get_account?account=`

v1 compatibility (nodeos history plugin, POST):

- `/v1/history/get_actions`
- `/v1/history/get_transaction`
- `/v1/history/get_key_accounts`
- `/v1/history/get_controlled_accounts`

## Not implemented (yet)

Relative to the original: the socket.io streaming API, RabbitMQ-based
multi-node scaling, index partitioning/lifecycle policies, the lightweight
explorer UI, and Hyperion's plugin system.

## Development

```bash
cargo test --workspace     # unit tests (codecs, decoder, name/asset math)
cargo clippy --workspace --all-targets
```
