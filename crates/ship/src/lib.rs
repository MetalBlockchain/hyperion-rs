//! Client for the Antelope state-history plugin (SHIP) websocket protocol.

pub mod client;
pub mod types;

pub use client::{ShipClient, ShipConfig};
pub use types::{
    AccountDelta, AccountRow, Action, ActionReceipt, ActionTrace, BlockHeader, BlockPosition,
    ContractRow, GetBlocksRequest, GetBlocksResult, GetStatusResult, PartialTransaction,
    PermissionLevel, PermissionRow, Result as ShipResultT, ShipError, ShipResult, TableDelta,
    TableDeltaRow, TransactionTrace,
};
