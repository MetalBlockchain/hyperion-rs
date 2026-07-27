//! Hand-written codecs for the state-history plugin wire types.
//!
//! The SHIP protocol frames everything as ABI-encoded `request` / `result`
//! variants. The shapes below track the Leap state-history ABI; unknown
//! variant versions are surfaced as errors rather than silently skipped so
//! an incompatible nodeos is noticed immediately.

use antelope::{keys, ByteReader, Name};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ShipError {
    #[error("antelope decode error: {0}")]
    Antelope(#[from] antelope::AntelopeError),
    #[error("unsupported {kind} variant index {index}")]
    UnsupportedVariant { kind: &'static str, index: u32 },
    #[error("websocket error: {0}")]
    WebSocket(Box<tokio_tungstenite::tungstenite::Error>),
    #[error("connection closed by server")]
    Closed,
    #[error("protocol error: {0}")]
    Protocol(String),
}

impl From<tokio_tungstenite::tungstenite::Error> for ShipError {
    fn from(e: tokio_tungstenite::tungstenite::Error) -> Self {
        ShipError::WebSocket(Box::new(e))
    }
}

pub type Result<T> = std::result::Result<T, ShipError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockPosition {
    pub block_num: u32,
    pub block_id: String,
}

impl BlockPosition {
    pub fn read(r: &mut ByteReader) -> Result<Self> {
        Ok(BlockPosition {
            block_num: r.read_u32()?,
            block_id: r.read_checksum256_hex()?,
        })
    }

    fn read_opt(r: &mut ByteReader) -> Result<Option<Self>> {
        Ok(if r.read_bool()? {
            Some(Self::read(r)?)
        } else {
            None
        })
    }
}

#[derive(Debug, Clone)]
pub struct GetStatusResult {
    pub head: BlockPosition,
    pub last_irreversible: BlockPosition,
    pub trace_begin_block: u32,
    pub trace_end_block: u32,
    pub chain_state_begin_block: u32,
    pub chain_state_end_block: u32,
    /// Present on newer nodeos releases (binary extension).
    pub chain_id: Option<String>,
}

#[derive(Debug)]
pub struct GetBlocksResult {
    pub head: BlockPosition,
    pub last_irreversible: BlockPosition,
    pub this_block: Option<BlockPosition>,
    pub prev_block: Option<BlockPosition>,
    /// Raw serialized `signed_block`, decoded lazily.
    pub block: Option<Vec<u8>>,
    /// Raw serialized `transaction_trace[]`, decoded lazily.
    pub traces: Option<Vec<u8>>,
    /// Raw serialized `table_delta[]`, decoded lazily.
    pub deltas: Option<Vec<u8>>,
}

#[derive(Debug)]
pub enum ShipResult {
    Status(GetStatusResult),
    Blocks(Box<GetBlocksResult>),
}

impl ShipResult {
    pub fn decode(data: &[u8]) -> Result<Self> {
        let mut r = ByteReader::new(data);
        match r.read_varuint32()? {
            0 => {
                let head = BlockPosition::read(&mut r)?;
                let last_irreversible = BlockPosition::read(&mut r)?;
                let trace_begin_block = r.read_u32()?;
                let trace_end_block = r.read_u32()?;
                let chain_state_begin_block = r.read_u32()?;
                let chain_state_end_block = r.read_u32()?;
                let chain_id = if r.is_empty() {
                    None
                } else {
                    Some(r.read_checksum256_hex()?)
                };
                Ok(ShipResult::Status(GetStatusResult {
                    head,
                    last_irreversible,
                    trace_begin_block,
                    trace_end_block,
                    chain_state_begin_block,
                    chain_state_end_block,
                    chain_id,
                }))
            }
            1 => {
                let head = BlockPosition::read(&mut r)?;
                let last_irreversible = BlockPosition::read(&mut r)?;
                let this_block = BlockPosition::read_opt(&mut r)?;
                let prev_block = BlockPosition::read_opt(&mut r)?;
                let read_opt_bytes = |r: &mut ByteReader| -> Result<Option<Vec<u8>>> {
                    Ok(if r.read_bool()? {
                        Some(r.read_bytes()?.to_vec())
                    } else {
                        None
                    })
                };
                // Field order per the SHIP ABI: block, traces, deltas.
                let block = read_opt_bytes(&mut r)?;
                let traces = read_opt_bytes(&mut r)?;
                let deltas = read_opt_bytes(&mut r)?;
                Ok(ShipResult::Blocks(Box::new(GetBlocksResult {
                    head,
                    last_irreversible,
                    this_block,
                    prev_block,
                    block,
                    traces,
                    deltas,
                })))
            }
            index => Err(ShipError::UnsupportedVariant {
                kind: "result",
                index,
            }),
        }
    }
}

// ---------------------------------------------------------------------------
// Requests (encoded, never decoded)
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

pub fn encode_get_status_request() -> Vec<u8> {
    vec![0] // variant index 0, no fields
}

pub struct GetBlocksRequest {
    pub start_block_num: u32,
    pub end_block_num: u32,
    pub max_messages_in_flight: u32,
    pub irreversible_only: bool,
    pub fetch_block: bool,
    pub fetch_traces: bool,
    pub fetch_deltas: bool,
}

pub fn encode_get_blocks_request(req: &GetBlocksRequest) -> Vec<u8> {
    let mut buf = Vec::with_capacity(32);
    push_varuint32(&mut buf, 1); // variant index: get_blocks_request_v0
    buf.extend_from_slice(&req.start_block_num.to_le_bytes());
    buf.extend_from_slice(&req.end_block_num.to_le_bytes());
    buf.extend_from_slice(&req.max_messages_in_flight.to_le_bytes());
    push_varuint32(&mut buf, 0); // have_positions: empty
    buf.push(req.irreversible_only as u8);
    buf.push(req.fetch_block as u8);
    buf.push(req.fetch_traces as u8);
    buf.push(req.fetch_deltas as u8);
    buf
}

pub fn encode_get_blocks_ack_request(num_messages: u32) -> Vec<u8> {
    let mut buf = Vec::with_capacity(8);
    push_varuint32(&mut buf, 2); // variant index: get_blocks_ack_request_v0
    buf.extend_from_slice(&num_messages.to_le_bytes());
    buf
}

// ---------------------------------------------------------------------------
// Transaction traces
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct PermissionLevel {
    pub actor: Name,
    pub permission: Name,
}

#[derive(Debug, Clone)]
pub struct Action {
    pub account: Name,
    pub name: Name,
    pub authorization: Vec<PermissionLevel>,
    pub data: Vec<u8>,
}

impl Action {
    fn read(r: &mut ByteReader) -> Result<Self> {
        let account = r.read_name()?;
        let name = r.read_name()?;
        let mut authorization = Vec::new();
        for _ in 0..r.read_varuint32()? {
            authorization.push(PermissionLevel {
                actor: r.read_name()?,
                permission: r.read_name()?,
            });
        }
        let data = r.read_bytes()?.to_vec();
        Ok(Action {
            account,
            name,
            authorization,
            data,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ActionReceipt {
    pub receiver: Name,
    pub act_digest: String,
    pub global_sequence: u64,
    pub recv_sequence: u64,
    pub auth_sequence: Vec<(Name, u64)>,
    pub code_sequence: u32,
    pub abi_sequence: u32,
}

impl ActionReceipt {
    fn read(r: &mut ByteReader) -> Result<Self> {
        match r.read_varuint32()? {
            0 => {
                let receiver = r.read_name()?;
                let act_digest = r.read_checksum256_hex()?;
                let global_sequence = r.read_u64()?;
                let recv_sequence = r.read_u64()?;
                let mut auth_sequence = Vec::new();
                for _ in 0..r.read_varuint32()? {
                    auth_sequence.push((r.read_name()?, r.read_u64()?));
                }
                let code_sequence = r.read_varuint32()?;
                let abi_sequence = r.read_varuint32()?;
                Ok(ActionReceipt {
                    receiver,
                    act_digest,
                    global_sequence,
                    recv_sequence,
                    auth_sequence,
                    code_sequence,
                    abi_sequence,
                })
            }
            index => Err(ShipError::UnsupportedVariant {
                kind: "action_receipt",
                index,
            }),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AccountDelta {
    pub account: Name,
    pub delta: i64,
}

fn read_account_deltas(r: &mut ByteReader) -> Result<Vec<AccountDelta>> {
    let mut out = Vec::new();
    for _ in 0..r.read_varuint32()? {
        out.push(AccountDelta {
            account: r.read_name()?,
            delta: r.read_i64()?,
        });
    }
    Ok(out)
}

#[derive(Debug, Clone)]
pub struct ActionTrace {
    pub action_ordinal: u32,
    pub creator_action_ordinal: u32,
    pub receipt: Option<ActionReceipt>,
    pub receiver: Name,
    pub act: Action,
    pub context_free: bool,
    pub elapsed: i64,
    pub console: String,
    pub account_ram_deltas: Vec<AccountDelta>,
    pub except: Option<String>,
    pub error_code: Option<u64>,
    /// Present in `action_trace_v1`.
    pub return_value: Option<Vec<u8>>,
}

impl ActionTrace {
    fn read(r: &mut ByteReader) -> Result<Self> {
        let version = r.read_varuint32()?;
        if version > 1 {
            return Err(ShipError::UnsupportedVariant {
                kind: "action_trace",
                index: version,
            });
        }
        let action_ordinal = r.read_varuint32()?;
        let creator_action_ordinal = r.read_varuint32()?;
        let receipt = if r.read_bool()? {
            Some(ActionReceipt::read(r)?)
        } else {
            None
        };
        let receiver = r.read_name()?;
        let act = Action::read(r)?;
        let context_free = r.read_bool()?;
        let elapsed = r.read_i64()?;
        let console = r.read_string()?;
        let account_ram_deltas = read_account_deltas(r)?;
        let except = if r.read_bool()? {
            Some(r.read_string()?)
        } else {
            None
        };
        let error_code = if r.read_bool()? {
            Some(r.read_u64()?)
        } else {
            None
        };
        let return_value = if version >= 1 {
            Some(r.read_bytes()?.to_vec())
        } else {
            None
        };
        Ok(ActionTrace {
            action_ordinal,
            creator_action_ordinal,
            receipt,
            receiver,
            act,
            context_free,
            elapsed,
            console,
            account_ram_deltas,
            except,
            error_code,
            return_value,
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct PartialTransaction {
    pub expiration: u32,
    pub ref_block_num: u16,
    pub ref_block_prefix: u32,
    pub max_net_usage_words: u32,
    pub max_cpu_usage_ms: u8,
    pub delay_sec: u32,
    pub signatures: Vec<String>,
}

impl PartialTransaction {
    fn read(r: &mut ByteReader) -> Result<Self> {
        let version = r.read_varuint32()?;
        if version > 1 {
            return Err(ShipError::UnsupportedVariant {
                kind: "partial_transaction",
                index: version,
            });
        }
        let mut pt = PartialTransaction {
            expiration: r.read_u32()?,
            ref_block_num: r.read_u16()?,
            ref_block_prefix: r.read_u32()?,
            max_net_usage_words: r.read_varuint32()?,
            max_cpu_usage_ms: r.read_u8()?,
            delay_sec: r.read_varuint32()?,
            ..Default::default()
        };
        // transaction_extensions
        for _ in 0..r.read_varuint32()? {
            r.read_u16()?;
            r.read_bytes()?;
        }
        if version == 0 {
            for _ in 0..r.read_varuint32()? {
                pt.signatures.push(keys::read_signature(r)?);
            }
            // context_free_data: bytes[]
            for _ in 0..r.read_varuint32()? {
                r.read_bytes()?;
            }
        } else {
            // partial_transaction_v1: optional prunable_data
            if r.read_bool()? {
                pt.read_prunable(r)?;
            }
        }
        Ok(pt)
    }

    fn read_prunable(&mut self, r: &mut ByteReader) -> Result<()> {
        // prunable_data_t variant:
        // 0 full_legacy { signatures, packed_context_free_data: bytes }
        // 1 none        { prunable_digest: checksum256 }
        // 2 partial     { signatures, context_free_segments: segment_type[] }
        // 3 full        { signatures, context_free_segments: bytes[] }
        let index = r.read_varuint32()?;
        match index {
            0 => {
                for _ in 0..r.read_varuint32()? {
                    self.signatures.push(keys::read_signature(r)?);
                }
                r.read_bytes()?;
            }
            1 => {
                r.read_exact(32)?;
            }
            2 | 3 => {
                for _ in 0..r.read_varuint32()? {
                    self.signatures.push(keys::read_signature(r)?);
                }
                for _ in 0..r.read_varuint32()? {
                    if index == 2 {
                        // segment_type variant: 0 digest checksum256, 1 bytes
                        match r.read_varuint32()? {
                            0 => {
                                r.read_exact(32)?;
                            }
                            1 => {
                                r.read_bytes()?;
                            }
                            i => {
                                return Err(ShipError::UnsupportedVariant {
                                    kind: "context_free_segment",
                                    index: i,
                                })
                            }
                        }
                    } else {
                        r.read_bytes()?;
                    }
                }
            }
            i => {
                return Err(ShipError::UnsupportedVariant {
                    kind: "prunable_data",
                    index: i,
                })
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct TransactionTrace {
    pub id: String,
    /// 0 = executed, 1 = soft_fail, 2 = hard_fail, 3 = delayed, 4 = expired.
    pub status: u8,
    pub cpu_usage_us: u32,
    pub net_usage_words: u32,
    pub elapsed: i64,
    pub net_usage: u64,
    pub scheduled: bool,
    pub action_traces: Vec<ActionTrace>,
    pub account_ram_delta: Option<AccountDelta>,
    pub except: Option<String>,
    pub error_code: Option<u64>,
    pub failed_dtrx_trace: Option<Box<TransactionTrace>>,
    pub partial: Option<PartialTransaction>,
}

impl TransactionTrace {
    pub fn status_name(status: u8) -> &'static str {
        match status {
            0 => "executed",
            1 => "soft_fail",
            2 => "hard_fail",
            3 => "delayed",
            4 => "expired",
            _ => "unknown",
        }
    }

    fn read(r: &mut ByteReader) -> Result<Self> {
        match r.read_varuint32()? {
            0 => {
                let id = r.read_checksum256_hex()?;
                let status = r.read_u8()?;
                let cpu_usage_us = r.read_u32()?;
                let net_usage_words = r.read_varuint32()?;
                let elapsed = r.read_i64()?;
                let net_usage = r.read_u64()?;
                let scheduled = r.read_bool()?;
                let mut action_traces = Vec::new();
                for _ in 0..r.read_varuint32()? {
                    action_traces.push(ActionTrace::read(r)?);
                }
                let account_ram_delta = if r.read_bool()? {
                    Some(AccountDelta {
                        account: r.read_name()?,
                        delta: r.read_i64()?,
                    })
                } else {
                    None
                };
                let except = if r.read_bool()? {
                    Some(r.read_string()?)
                } else {
                    None
                };
                let error_code = if r.read_bool()? {
                    Some(r.read_u64()?)
                } else {
                    None
                };
                let failed_dtrx_trace = if r.read_bool()? {
                    Some(Box::new(TransactionTrace::read(r)?))
                } else {
                    None
                };
                let partial = if r.read_bool()? {
                    Some(PartialTransaction::read(r)?)
                } else {
                    None
                };
                Ok(TransactionTrace {
                    id,
                    status,
                    cpu_usage_us,
                    net_usage_words,
                    elapsed,
                    net_usage,
                    scheduled,
                    action_traces,
                    account_ram_delta,
                    except,
                    error_code,
                    failed_dtrx_trace,
                    partial,
                })
            }
            index => Err(ShipError::UnsupportedVariant {
                kind: "transaction_trace",
                index,
            }),
        }
    }

    /// Decode the `traces` payload of a `get_blocks_result`.
    pub fn decode_traces(data: &[u8]) -> Result<Vec<TransactionTrace>> {
        let mut r = ByteReader::new(data);
        let mut out = Vec::new();
        for _ in 0..r.read_varuint32()? {
            out.push(TransactionTrace::read(&mut r)?);
        }
        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// Table deltas
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct TableDeltaRow {
    pub present: bool,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct TableDelta {
    pub name: String,
    pub rows: Vec<TableDeltaRow>,
}

impl TableDelta {
    /// Decode the `deltas` payload of a `get_blocks_result`.
    pub fn decode_deltas(data: &[u8]) -> Result<Vec<TableDelta>> {
        let mut r = ByteReader::new(data);
        let mut out = Vec::new();
        for _ in 0..r.read_varuint32()? {
            match r.read_varuint32()? {
                0 => {
                    let name = r.read_string()?;
                    let mut rows = Vec::new();
                    for _ in 0..r.read_varuint32()? {
                        let present = r.read_bool()?;
                        let data = r.read_bytes()?.to_vec();
                        rows.push(TableDeltaRow { present, data });
                    }
                    out.push(TableDelta { name, rows });
                }
                index => {
                    return Err(ShipError::UnsupportedVariant {
                        kind: "table_delta",
                        index,
                    })
                }
            }
        }
        Ok(out)
    }
}

/// A `contract_row_v0` from the `contract_row` table.
#[derive(Debug, Clone)]
pub struct ContractRow {
    pub code: Name,
    pub scope: Name,
    pub table: Name,
    pub primary_key: u64,
    pub payer: Name,
    pub value: Vec<u8>,
}

impl ContractRow {
    pub fn decode(data: &[u8]) -> Result<Self> {
        let mut r = ByteReader::new(data);
        match r.read_varuint32()? {
            0 => Ok(ContractRow {
                code: r.read_name()?,
                scope: r.read_name()?,
                table: r.read_name()?,
                primary_key: r.read_u64()?,
                payer: r.read_name()?,
                value: r.read_bytes()?.to_vec(),
            }),
            index => Err(ShipError::UnsupportedVariant {
                kind: "contract_row",
                index,
            }),
        }
    }
}

/// An `account_v0` from the `account` table — carries contract ABI updates.
#[derive(Debug, Clone)]
pub struct AccountRow {
    pub name: Name,
    pub creation_date_slot: u32,
    pub abi: Vec<u8>,
}

impl AccountRow {
    pub fn decode(data: &[u8]) -> Result<Self> {
        let mut r = ByteReader::new(data);
        match r.read_varuint32()? {
            0 => Ok(AccountRow {
                name: r.read_name()?,
                creation_date_slot: r.read_u32()?,
                abi: r.read_bytes()?.to_vec(),
            }),
            index => Err(ShipError::UnsupportedVariant {
                kind: "account",
                index,
            }),
        }
    }
}

/// A `permission_v0` from the `permission` table — feeds `get_key_accounts`.
#[derive(Debug, Clone)]
pub struct PermissionRow {
    pub owner: Name,
    pub name: Name,
    pub parent: Name,
    /// time_point (microseconds since epoch).
    pub last_updated: i64,
    pub threshold: u32,
    pub keys: Vec<String>,
    /// Controlling accounts as `actor@permission`.
    pub accounts: Vec<String>,
}

impl PermissionRow {
    pub fn decode(data: &[u8]) -> Result<Self> {
        let mut r = ByteReader::new(data);
        match r.read_varuint32()? {
            0 => {
                let owner = r.read_name()?;
                let name = r.read_name()?;
                let parent = r.read_name()?;
                let last_updated = r.read_i64()?;
                // authority
                let threshold = r.read_u32()?;
                let mut keys = Vec::new();
                for _ in 0..r.read_varuint32()? {
                    keys.push(keys::read_public_key(&mut r)?);
                    r.read_u16()?; // weight
                }
                let mut accounts = Vec::new();
                for _ in 0..r.read_varuint32()? {
                    let actor = r.read_name()?;
                    let permission = r.read_name()?;
                    r.read_u16()?; // weight
                    accounts.push(format!("{actor}@{permission}"));
                }
                // waits
                for _ in 0..r.read_varuint32()? {
                    r.read_u32()?;
                    r.read_u16()?;
                }
                Ok(PermissionRow {
                    owner,
                    name,
                    parent,
                    last_updated,
                    threshold,
                    keys,
                    accounts,
                })
            }
            index => Err(ShipError::UnsupportedVariant {
                kind: "permission",
                index,
            }),
        }
    }
}

// ---------------------------------------------------------------------------
// Signed block header
// ---------------------------------------------------------------------------

/// The subset of `signed_block` Hyperion indexes per block. Parsing stops
/// after counting transaction receipts.
#[derive(Debug, Clone)]
pub struct BlockHeader {
    pub timestamp_slot: u32,
    pub producer: Name,
    pub confirmed: u16,
    pub previous: String,
    pub transaction_mroot: String,
    pub action_mroot: String,
    pub schedule_version: u32,
    pub producer_signature: String,
    pub trx_count: u32,
    pub cpu_usage_us: u64,
    pub net_usage_words: u64,
}

impl BlockHeader {
    pub fn decode(data: &[u8]) -> Result<Self> {
        let mut r = ByteReader::new(data);
        let timestamp_slot = r.read_u32()?;
        let producer = r.read_name()?;
        let confirmed = r.read_u16()?;
        let previous = r.read_checksum256_hex()?;
        let transaction_mroot = r.read_checksum256_hex()?;
        let action_mroot = r.read_checksum256_hex()?;
        let schedule_version = r.read_u32()?;
        // new_producers: optional legacy producer schedule
        if r.read_bool()? {
            r.read_u32()?; // version
            for _ in 0..r.read_varuint32()? {
                r.read_name()?;
                keys::read_public_key(&mut r)?;
            }
        }
        // header_extensions
        for _ in 0..r.read_varuint32()? {
            r.read_u16()?;
            r.read_bytes()?;
        }
        let producer_signature = keys::read_signature(&mut r)?;

        let mut trx_count = 0u32;
        let mut cpu_usage_us = 0u64;
        let mut net_usage_words = 0u64;
        for _ in 0..r.read_varuint32()? {
            // transaction_receipt: status, cpu, net, trx variant
            r.read_u8()?;
            cpu_usage_us += r.read_u32()? as u64;
            net_usage_words += r.read_varuint32()? as u64;
            match r.read_varuint32()? {
                0 => {
                    r.read_exact(32)?; // transaction_id
                }
                1 => {
                    // packed_transaction
                    for _ in 0..r.read_varuint32()? {
                        keys::read_signature(&mut r)?;
                    }
                    r.read_u8()?; // compression
                    r.read_bytes()?; // packed_context_free_data
                    r.read_bytes()?; // packed_trx
                }
                index => {
                    return Err(ShipError::UnsupportedVariant {
                        kind: "transaction_receipt",
                        index,
                    })
                }
            }
            trx_count += 1;
        }
        Ok(BlockHeader {
            timestamp_slot,
            producer,
            confirmed,
            previous,
            transaction_mroot,
            action_mroot,
            schedule_version,
            producer_signature,
            trx_count,
            cpu_usage_us,
            net_usage_words,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_get_blocks_request() {
        let req = GetBlocksRequest {
            start_block_num: 100,
            end_block_num: u32::MAX,
            max_messages_in_flight: 4,
            irreversible_only: false,
            fetch_block: true,
            fetch_traces: true,
            fetch_deltas: true,
        };
        let buf = encode_get_blocks_request(&req);
        assert_eq!(buf[0], 1); // variant index
        assert_eq!(&buf[1..5], &100u32.to_le_bytes());
        assert_eq!(&buf[5..9], &u32::MAX.to_le_bytes());
        assert_eq!(&buf[9..13], &4u32.to_le_bytes());
        assert_eq!(&buf[13..], &[0, 0, 1, 1, 1]);
    }

    // Test-side encoder helpers to build binary fixtures.
    fn push_varuint32(buf: &mut Vec<u8>, v: u32) {
        super::push_varuint32(buf, v)
    }

    fn push_name(buf: &mut Vec<u8>, name: &str) {
        buf.extend_from_slice(&name.parse::<Name>().unwrap().0.to_le_bytes());
    }

    fn push_bytes(buf: &mut Vec<u8>, data: &[u8]) {
        push_varuint32(buf, data.len() as u32);
        buf.extend_from_slice(data);
    }

    fn sample_trace_bytes() -> Vec<u8> {
        let mut buf = Vec::new();
        push_varuint32(&mut buf, 1); // one trace
        push_varuint32(&mut buf, 0); // transaction_trace_v0
        buf.extend_from_slice(&[0xaa; 32]); // id
        buf.push(0); // status executed
        buf.extend_from_slice(&150u32.to_le_bytes()); // cpu_usage_us
        push_varuint32(&mut buf, 12); // net_usage_words
        buf.extend_from_slice(&2000i64.to_le_bytes()); // elapsed
        buf.extend_from_slice(&96u64.to_le_bytes()); // net_usage
        buf.push(0); // scheduled = false
        push_varuint32(&mut buf, 1); // one action trace
        {
            push_varuint32(&mut buf, 1); // action_trace_v1
            push_varuint32(&mut buf, 1); // action_ordinal
            push_varuint32(&mut buf, 0); // creator_action_ordinal
            buf.push(1); // receipt present
            push_varuint32(&mut buf, 0); // action_receipt_v0
            push_name(&mut buf, "eosio.token"); // receiver
            buf.extend_from_slice(&[0xbb; 32]); // act_digest
            buf.extend_from_slice(&777u64.to_le_bytes()); // global_sequence
            buf.extend_from_slice(&3u64.to_le_bytes()); // recv_sequence
            push_varuint32(&mut buf, 1); // auth_sequence
            push_name(&mut buf, "alice");
            buf.extend_from_slice(&9u64.to_le_bytes());
            push_varuint32(&mut buf, 2); // code_sequence
            push_varuint32(&mut buf, 1); // abi_sequence
            push_name(&mut buf, "eosio.token"); // receiver (of act trace)
                                                // act
            push_name(&mut buf, "eosio.token"); // account
            push_name(&mut buf, "transfer"); // name
            push_varuint32(&mut buf, 1); // one authorization
            push_name(&mut buf, "alice");
            push_name(&mut buf, "active");
            push_bytes(&mut buf, &[1, 2, 3]); // data
            buf.push(0); // context_free
            buf.extend_from_slice(&42i64.to_le_bytes()); // elapsed
            push_bytes(&mut buf, b""); // console
            push_varuint32(&mut buf, 0); // account_ram_deltas
            buf.push(0); // except: none
            buf.push(0); // error_code: none
            push_bytes(&mut buf, &[]); // return_value (v1)
        }
        buf.push(0); // account_ram_delta: none
        buf.push(0); // except: none
        buf.push(0); // error_code: none
        buf.push(0); // failed_dtrx_trace: none
        buf.push(1); // partial present
        {
            push_varuint32(&mut buf, 0); // partial_transaction_v0
            buf.extend_from_slice(&1700000000u32.to_le_bytes()); // expiration
            buf.extend_from_slice(&12u16.to_le_bytes()); // ref_block_num
            buf.extend_from_slice(&34u32.to_le_bytes()); // ref_block_prefix
            push_varuint32(&mut buf, 0); // max_net_usage_words
            buf.push(0); // max_cpu_usage_ms
            push_varuint32(&mut buf, 0); // delay_sec
            push_varuint32(&mut buf, 0); // transaction_extensions
            push_varuint32(&mut buf, 1); // one signature
            buf.push(0); // K1
            buf.extend_from_slice(&[0x11; 65]);
            push_varuint32(&mut buf, 0); // context_free_data
        }
        buf
    }

    #[test]
    fn decodes_transaction_trace() {
        let traces = TransactionTrace::decode_traces(&sample_trace_bytes()).unwrap();
        assert_eq!(traces.len(), 1);
        let t = &traces[0];
        assert_eq!(t.id, "aa".repeat(32));
        assert_eq!(t.status, 0);
        assert_eq!(t.cpu_usage_us, 150);
        assert_eq!(t.action_traces.len(), 1);
        let at = &t.action_traces[0];
        assert_eq!(at.receiver.to_string(), "eosio.token");
        assert_eq!(at.act.name.to_string(), "transfer");
        assert_eq!(at.act.authorization[0].actor.to_string(), "alice");
        assert_eq!(at.receipt.as_ref().unwrap().global_sequence, 777);
        assert_eq!(at.act.data, vec![1, 2, 3]);
        let partial = t.partial.as_ref().unwrap();
        assert_eq!(partial.ref_block_num, 12);
        assert_eq!(partial.signatures.len(), 1);
        assert!(partial.signatures[0].starts_with("SIG_K1_"));
    }

    #[test]
    fn decodes_table_deltas() {
        let mut buf = Vec::new();
        push_varuint32(&mut buf, 1); // one delta
        push_varuint32(&mut buf, 0); // table_delta_v0
        push_bytes(&mut buf, b"contract_row"); // name
        push_varuint32(&mut buf, 1); // one row
        buf.push(1); // present
        let mut row = Vec::new();
        push_varuint32(&mut row, 0); // contract_row_v0
        push_name(&mut row, "eosio.token"); // code
        push_name(&mut row, "alice"); // scope
        push_name(&mut row, "accounts"); // table
        row.extend_from_slice(&5u64.to_le_bytes()); // primary_key
        push_name(&mut row, "alice"); // payer
        push_bytes(&mut row, &[9, 9]); // value
        push_bytes(&mut buf, &row);

        let deltas = TableDelta::decode_deltas(&buf).unwrap();
        assert_eq!(deltas.len(), 1);
        assert_eq!(deltas[0].name, "contract_row");
        let cr = ContractRow::decode(&deltas[0].rows[0].data).unwrap();
        assert_eq!(cr.code.to_string(), "eosio.token");
        assert_eq!(cr.scope.to_string(), "alice");
        assert_eq!(cr.table.to_string(), "accounts");
        assert_eq!(cr.primary_key, 5);
        assert_eq!(cr.value, vec![9, 9]);
    }

    #[test]
    fn decodes_get_blocks_result() {
        let mut buf = Vec::new();
        push_varuint32(&mut buf, 1); // result variant: get_blocks_result_v0
        buf.extend_from_slice(&500u32.to_le_bytes());
        buf.extend_from_slice(&[0x01; 32]);
        buf.extend_from_slice(&400u32.to_le_bytes());
        buf.extend_from_slice(&[0x02; 32]);
        buf.push(1); // this_block present
        buf.extend_from_slice(&450u32.to_le_bytes());
        buf.extend_from_slice(&[0x03; 32]);
        buf.push(1); // prev_block present
        buf.extend_from_slice(&449u32.to_le_bytes());
        buf.extend_from_slice(&[0x04; 32]);
        buf.push(0); // block absent
        buf.push(1); // traces present
        push_bytes(&mut buf, &[0x00]); // empty vector of traces
        buf.push(0); // deltas absent

        match ShipResult::decode(&buf).unwrap() {
            ShipResult::Blocks(b) => {
                assert_eq!(b.head.block_num, 500);
                assert_eq!(b.this_block.as_ref().unwrap().block_num, 450);
                assert_eq!(b.prev_block.as_ref().unwrap().block_num, 449);
                assert!(b.block.is_none());
                assert_eq!(b.traces.as_deref(), Some(&[0x00][..]));
                assert!(b.deltas.is_none());
            }
            other => panic!("expected blocks result, got {other:?}"),
        }
    }
}
