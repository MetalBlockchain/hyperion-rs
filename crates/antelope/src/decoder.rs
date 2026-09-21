//! ABI-driven binary decoding: given an [`Abi`] and a type name, decode an
//! Antelope-serialized buffer into `serde_json::Value`, following the same
//! conventions as abieos (names/checksums/assets as strings, integers up to
//! 64 bits as JSON numbers, 128-bit integers as decimal strings).

use crate::{keys, time, Abi, AntelopeError, Asset, ByteReader, Name, Result, Symbol, SymbolCode};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

enum AbiSource<'a> {
    Borrowed(&'a Abi),
    Shared(Arc<Abi>),
}

impl Deref for AbiSource<'_> {
    type Target = Abi;

    fn deref(&self) -> &Abi {
        match self {
            Self::Borrowed(abi) => abi,
            Self::Shared(abi) => abi,
        }
    }
}

pub struct AbiDecoder<'a> {
    abi: AbiSource<'a>,
    aliases: HashMap<String, usize>,
    structs: HashMap<String, usize>,
    variants: HashMap<String, usize>,
    actions: HashMap<Name, usize>,
    tables: HashMap<Name, usize>,
}

const MAX_DEPTH: u32 = 64;

impl<'a> AbiDecoder<'a> {
    pub fn new(abi: &'a Abi) -> Self {
        Self::build(AbiSource::Borrowed(abi))
    }

    /// Prepare a reusable decoder that owns a shared ABI. Lookup tables are
    /// built once and reused across actions, rows, and blocks.
    pub fn from_shared(abi: Arc<Abi>) -> AbiDecoder<'static> {
        AbiDecoder::build(AbiSource::Shared(abi))
    }

    fn build(abi: AbiSource<'a>) -> Self {
        AbiDecoder {
            aliases: abi
                .types
                .iter()
                .enumerate()
                .map(|(i, t)| (t.new_type_name.clone(), i))
                .collect(),
            structs: abi
                .structs
                .iter()
                .enumerate()
                .map(|(i, s)| (s.name.clone(), i))
                .collect(),
            variants: abi
                .variants
                .iter()
                .enumerate()
                .map(|(i, v)| (v.name.clone(), i))
                .collect(),
            // Reverse iteration preserves the first-match semantics of ABI lookups.
            actions: abi
                .actions
                .iter()
                .enumerate()
                .rev()
                .map(|(i, a)| (a.name, i))
                .collect(),
            tables: abi
                .tables
                .iter()
                .enumerate()
                .rev()
                .map(|(i, t)| (t.name, i))
                .collect(),
            abi,
        }
    }

    /// Decode `data` as the given ABI type. Fails if the type is unknown or
    /// the buffer is malformed; trailing bytes are tolerated (some contracts
    /// over-serialize).
    pub fn decode(&self, type_name: &str, data: &[u8]) -> Result<Value> {
        let mut reader = ByteReader::new(data);
        self.decode_type(type_name, &mut reader, 0)
    }

    /// Decode the data payload of an action, resolving the action's struct
    /// type from the ABI.
    pub fn decode_action(&self, action: Name, data: &[u8]) -> Result<Value> {
        let index = self
            .actions
            .get(&action)
            .ok_or_else(|| AntelopeError::UnknownType(format!("action {action}")))?;
        self.decode(&self.abi.actions[*index].type_, data)
    }

    /// Decode a table row, resolving the row struct type from the ABI.
    pub fn decode_table_row(&self, table: Name, data: &[u8]) -> Result<Value> {
        let index = self
            .tables
            .get(&table)
            .ok_or_else(|| AntelopeError::UnknownType(format!("table {table}")))?;
        self.decode(&self.abi.tables[*index].type_, data)
    }

    fn decode_type(&self, type_name: &str, r: &mut ByteReader, depth: u32) -> Result<Value> {
        if depth > MAX_DEPTH {
            return Err(AntelopeError::BadAbi("type nesting too deep".into()));
        }

        // Suffix modifiers bind tightest-last: `foo[]?` is optional-array.
        if let Some(inner) = type_name.strip_suffix('$') {
            // Binary extension: absent iff the buffer is exhausted.
            if r.is_empty() {
                return Ok(Value::Null);
            }
            return self.decode_type(inner, r, depth + 1);
        }
        if let Some(inner) = type_name.strip_suffix('?') {
            if !r.read_bool()? {
                return Ok(Value::Null);
            }
            return self.decode_type(inner, r, depth + 1);
        }
        if let Some(inner) = type_name.strip_suffix("[]") {
            let count = r.read_varuint32()?;
            let mut items = Vec::with_capacity(count.min(4096) as usize);
            for _ in 0..count {
                items.push(self.decode_type(inner, r, depth + 1)?);
            }
            return Ok(Value::Array(items));
        }

        if let Some(value) = self.decode_builtin(type_name, r, depth)? {
            return Ok(value);
        }
        if let Some(&index) = self.structs.get(type_name) {
            return self.decode_struct(&self.abi.structs[index], r, depth);
        }
        if let Some(&index) = self.variants.get(type_name) {
            let def = &self.abi.variants[index];
            let index = r.read_varuint32()?;
            let inner =
                def.types
                    .get(index as usize)
                    .ok_or_else(|| AntelopeError::BadVariantIndex {
                        variant: def.name.clone(),
                        index,
                    })?;
            let value = self.decode_type(inner, r, depth + 1)?;
            return Ok(json!([inner, value]));
        }
        // Aliases may point at modified types (e.g. `permission[]`), so
        // resolve one step and re-enter; the depth guard breaks cycles.
        if let Some(&index) = self.aliases.get(type_name) {
            return self.decode_type(&self.abi.types[index].type_, r, depth + 1);
        }
        Err(AntelopeError::UnknownType(type_name.to_string()))
    }

    fn decode_struct(
        &self,
        def: &crate::abi::StructDef,
        r: &mut ByteReader,
        depth: u32,
    ) -> Result<Value> {
        let mut obj = serde_json::Map::new();
        if !def.base.is_empty() {
            let base = self.decode_type(&def.base, r, depth + 1)?;
            if let Value::Object(base_fields) = base {
                obj.extend(base_fields);
            }
        }
        for field in &def.fields {
            let value = self.decode_type(&field.type_, r, depth + 1)?;
            obj.insert(field.name.clone(), value);
        }
        Ok(Value::Object(obj))
    }

    fn decode_builtin(&self, name: &str, r: &mut ByteReader, depth: u32) -> Result<Option<Value>> {
        let value = match name {
            "bool" => json!(r.read_bool()?),
            "int8" => json!(r.read_i8()?),
            "uint8" => json!(r.read_u8()?),
            "int16" => json!(r.read_i16()?),
            "uint16" => json!(r.read_u16()?),
            "int32" => json!(r.read_i32()?),
            "uint32" => json!(r.read_u32()?),
            "int64" => json!(r.read_i64()?),
            "uint64" => json!(r.read_u64()?),
            "int128" => json!(r.read_i128()?.to_string()),
            "uint128" => json!(r.read_u128()?.to_string()),
            "varuint32" => json!(r.read_varuint32()?),
            "varint32" => json!(r.read_varint32()?),
            "float32" => json!(r.read_f32()?),
            "float64" => json!(r.read_f64()?),
            "float128" => json!(hex::encode(r.read_exact(16)?)),
            "time_point" => json!(time::time_point_to_string(r.read_i64()?)),
            "time_point_sec" => json!(time::time_point_sec_to_string(r.read_u32()?)),
            "block_timestamp_type" => json!(time::block_timestamp_to_string(r.read_u32()?)),
            "name" => json!(r.read_name()?.to_string()),
            "bytes" => json!(hex::encode(r.read_bytes()?)),
            "string" => json!(r.read_string()?),
            "checksum160" => json!(hex::encode(r.read_exact(20)?)),
            "checksum256" => json!(hex::encode(r.read_exact(32)?)),
            "checksum512" => json!(hex::encode(r.read_exact(64)?)),
            "public_key" => json!(keys::read_public_key(r)?),
            "signature" => json!(keys::read_signature(r)?),
            "symbol" => json!(Symbol(r.read_u64()?).to_string()),
            "symbol_code" => json!(SymbolCode(r.read_u64()?).to_string()),
            "asset" => {
                let amount = r.read_i64()?;
                let symbol = Symbol(r.read_u64()?);
                json!(Asset::new(amount, symbol).to_string())
            }
            "extended_asset" => {
                let amount = r.read_i64()?;
                let symbol = Symbol(r.read_u64()?);
                let contract = r.read_name()?;
                json!({
                    "quantity": Asset::new(amount, symbol).to_string(),
                    "contract": contract.to_string(),
                })
            }
            _ => {
                let _ = depth;
                return Ok(None);
            }
        };
        Ok(Some(value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn token_abi() -> Abi {
        serde_json::from_value(json!({
            "version": "eosio::abi/1.1",
            "types": [{"new_type_name": "account_name", "type": "name"}],
            "structs": [
                {
                    "name": "transfer",
                    "base": "",
                    "fields": [
                        {"name": "from", "type": "account_name"},
                        {"name": "to", "type": "account_name"},
                        {"name": "quantity", "type": "asset"},
                        {"name": "memo", "type": "string"}
                    ]
                },
                {
                    "name": "account",
                    "base": "",
                    "fields": [{"name": "balance", "type": "asset"}]
                }
            ],
            "actions": [{"name": "transfer", "type": "transfer", "ricardian_contract": ""}],
            "tables": [{"name": "accounts", "index_type": "i64", "key_names": [], "key_types": [], "type": "account"}],
            "variants": []
        }))
        .unwrap()
    }

    #[test]
    fn decodes_token_transfer() {
        let abi = token_abi();
        let decoder = AbiDecoder::new(&abi);
        // transfer{from: eosio, to: eosio.token, quantity: 1.0000 EOS, memo: "hi"}
        let mut data = Vec::new();
        data.extend_from_slice(&"eosio".parse::<Name>().unwrap().0.to_le_bytes());
        data.extend_from_slice(&"eosio.token".parse::<Name>().unwrap().0.to_le_bytes());
        data.extend_from_slice(&10000i64.to_le_bytes());
        data.extend_from_slice(&"4,EOS".parse::<Symbol>().unwrap().0.to_le_bytes());
        data.push(2);
        data.extend_from_slice(b"hi");

        let value = decoder
            .decode_action("transfer".parse().unwrap(), &data)
            .unwrap();
        assert_eq!(
            value,
            json!({
                "from": "eosio",
                "to": "eosio.token",
                "quantity": "1.0000 EOS",
                "memo": "hi"
            })
        );
    }

    #[test]
    fn decodes_table_row() {
        let abi = token_abi();
        let decoder = AbiDecoder::new(&abi);
        let mut data = Vec::new();
        data.extend_from_slice(&123456i64.to_le_bytes());
        data.extend_from_slice(&"4,EOS".parse::<Symbol>().unwrap().0.to_le_bytes());
        let value = decoder
            .decode_table_row("accounts".parse().unwrap(), &data)
            .unwrap();
        assert_eq!(value, json!({"balance": "12.3456 EOS"}));
    }

    #[test]
    fn optional_array_and_variant() {
        let abi: Abi = serde_json::from_value(json!({
            "version": "eosio::abi/1.1",
            "types": [],
            "structs": [
                {"name": "holder", "base": "", "fields": [
                    {"name": "ids", "type": "uint32[]"},
                    {"name": "note", "type": "string?"},
                    {"name": "extra", "type": "choice$"}
                ]}
            ],
            "actions": [],
            "tables": [],
            "variants": [{"name": "choice", "types": ["uint8", "string"]}]
        }))
        .unwrap();
        let decoder = AbiDecoder::new(&abi);

        // ids=[7], note=None, extra omitted (extension)
        let data = [1u8, 7, 0, 0, 0, 0];
        let v = decoder.decode("holder", &data).unwrap();
        assert_eq!(v, json!({"ids": [7], "note": null, "extra": null}));

        // ids=[], note=Some("x"), extra=variant 1 -> string "y"
        let data = [0u8, 1, 1, b'x', 1, 1, b'y'];
        let v = decoder.decode("holder", &data).unwrap();
        assert_eq!(v, json!({"ids": [], "note": "x", "extra": ["string", "y"]}));
    }

    #[test]
    fn unknown_type_errors() {
        let abi = token_abi();
        let decoder = AbiDecoder::new(&abi);
        assert!(decoder.decode("nonexistent", &[]).is_err());
    }
}
