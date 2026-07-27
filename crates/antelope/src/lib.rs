//! Core Antelope (EOSIO) chain types and ABI-driven binary decoding.
//!
//! This crate provides the primitives Hyperion needs to understand the
//! state-history byte streams produced by nodeos: the `name`/`symbol`/`asset`
//! encodings, a little-endian binary reader for the Antelope serialization
//! format, and an ABI-driven decoder that turns raw action data and table
//! rows into JSON.

pub mod abi;
pub mod asset;
pub mod decoder;
pub mod keys;
pub mod name;
pub mod reader;
pub mod time;

pub use abi::Abi;
pub use asset::{Asset, Symbol, SymbolCode};
pub use decoder::AbiDecoder;
pub use name::Name;
pub use reader::ByteReader;

#[derive(Debug, thiserror::Error)]
pub enum AntelopeError {
    #[error("unexpected end of buffer: needed {needed} bytes, {remaining} remaining")]
    Eof { needed: usize, remaining: usize },
    #[error("varuint32 too long or malformed")]
    BadVarint,
    #[error("invalid name: {0}")]
    BadName(String),
    #[error("invalid symbol: {0}")]
    BadSymbol(String),
    #[error("invalid utf-8 in string")]
    BadUtf8,
    #[error("unknown ABI type: {0}")]
    UnknownType(String),
    #[error("invalid ABI: {0}")]
    BadAbi(String),
    #[error("unsupported key/signature type {0}")]
    BadKeyType(u8),
    #[error("variant index {index} out of range for {variant}")]
    BadVariantIndex { variant: String, index: u32 },
}

pub type Result<T> = std::result::Result<T, AntelopeError>;
