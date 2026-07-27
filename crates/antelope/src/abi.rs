//! The Antelope `abi_def` model, loadable from JSON (chain API `get_abi`) or
//! from the packed binary form carried in `setabi` actions and state-history
//! `account` table deltas.

use crate::{ByteReader, Name, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TypeDef {
    pub new_type_name: String,
    #[serde(rename = "type")]
    pub type_: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FieldDef {
    pub name: String,
    #[serde(rename = "type")]
    pub type_: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StructDef {
    pub name: String,
    #[serde(default)]
    pub base: String,
    #[serde(default)]
    pub fields: Vec<FieldDef>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ActionDef {
    pub name: Name,
    #[serde(rename = "type")]
    pub type_: String,
    #[serde(default)]
    pub ricardian_contract: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TableDef {
    pub name: Name,
    #[serde(default)]
    pub index_type: String,
    #[serde(default)]
    pub key_names: Vec<String>,
    #[serde(default)]
    pub key_types: Vec<String>,
    #[serde(rename = "type")]
    pub type_: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VariantDef {
    pub name: String,
    #[serde(default)]
    pub types: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Abi {
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub types: Vec<TypeDef>,
    #[serde(default)]
    pub structs: Vec<StructDef>,
    #[serde(default)]
    pub actions: Vec<ActionDef>,
    #[serde(default)]
    pub tables: Vec<TableDef>,
    #[serde(default)]
    pub variants: Vec<VariantDef>,
}

impl Abi {
    pub fn from_json(value: &serde_json::Value) -> std::result::Result<Self, serde_json::Error> {
        serde_json::from_value(value.clone())
    }

    /// Decode a packed (binary) `abi_def`, as found in `setabi` action data
    /// and in the state-history `account` table.
    pub fn from_bin(data: &[u8]) -> Result<Self> {
        let mut r = ByteReader::new(data);
        let version = r.read_string()?;

        let mut abi = Abi {
            version,
            ..Default::default()
        };

        for _ in 0..r.read_varuint32()? {
            abi.types.push(TypeDef {
                new_type_name: r.read_string()?,
                type_: r.read_string()?,
            });
        }
        for _ in 0..r.read_varuint32()? {
            let name = r.read_string()?;
            let base = r.read_string()?;
            let mut fields = Vec::new();
            for _ in 0..r.read_varuint32()? {
                fields.push(FieldDef {
                    name: r.read_string()?,
                    type_: r.read_string()?,
                });
            }
            abi.structs.push(StructDef { name, base, fields });
        }
        for _ in 0..r.read_varuint32()? {
            abi.actions.push(ActionDef {
                name: r.read_name()?,
                type_: r.read_string()?,
                ricardian_contract: r.read_string()?,
            });
        }
        for _ in 0..r.read_varuint32()? {
            let name = r.read_name()?;
            let index_type = r.read_string()?;
            let mut key_names = Vec::new();
            for _ in 0..r.read_varuint32()? {
                key_names.push(r.read_string()?);
            }
            let mut key_types = Vec::new();
            for _ in 0..r.read_varuint32()? {
                key_types.push(r.read_string()?);
            }
            let type_ = r.read_string()?;
            abi.tables.push(TableDef {
                name,
                index_type,
                key_names,
                key_types,
                type_,
            });
        }
        // ricardian_clauses
        for _ in 0..r.read_varuint32()? {
            r.read_string()?;
            r.read_string()?;
        }
        // error_messages
        for _ in 0..r.read_varuint32()? {
            r.read_u64()?;
            r.read_string()?;
        }
        // abi_extensions
        for _ in 0..r.read_varuint32()? {
            r.read_u16()?;
            r.read_bytes()?;
        }
        // variants (binary extension, abi/1.1+)
        if !r.is_empty() {
            for _ in 0..r.read_varuint32()? {
                let name = r.read_string()?;
                let mut types = Vec::new();
                for _ in 0..r.read_varuint32()? {
                    types.push(r.read_string()?);
                }
                abi.variants.push(VariantDef { name, types });
            }
        }
        // action_results (abi/1.2+) — parsed and discarded.
        Ok(abi)
    }

    /// The struct type name for a given action, if declared.
    pub fn action_type(&self, action: Name) -> Option<&str> {
        self.actions
            .iter()
            .find(|a| a.name == action)
            .map(|a| a.type_.as_str())
    }

    /// The row struct type name for a given table, if declared.
    pub fn table_type(&self, table: Name) -> Option<&str> {
        self.tables
            .iter()
            .find(|t| t.name == table)
            .map(|t| t.type_.as_str())
    }
}
