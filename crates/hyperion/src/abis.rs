//! Contract ABI cache.
//!
//! During linear indexing, ABI updates arrive in block order via the
//! state-history `account` table deltas, so the cache always reflects the
//! chain state at the block being processed. On a cache miss (e.g. the
//! indexer started mid-chain) the current ABI is fetched from the chain API
//! — a pragmatic fallback that can mismatch very old blocks.

use crate::chain_client::ChainClient;
use antelope::{Abi, AbiDecoder, Name};
use std::collections::HashMap;
use std::sync::Arc;

pub struct AbiCache {
    chain: ChainClient,
    /// `None` means "known to have no (usable) ABI" — negative cache.
    abis: HashMap<u64, Option<Arc<Abi>>>,
    decoders: HashMap<u64, Arc<AbiDecoder<'static>>>,
}

impl AbiCache {
    pub fn new(chain: ChainClient) -> Self {
        AbiCache {
            chain,
            abis: HashMap::new(),
            decoders: HashMap::new(),
        }
    }

    /// Record an ABI update seen on-chain (packed `abi_def` bytes; empty
    /// bytes mean the ABI was cleared).
    pub fn update(&mut self, account: Name, packed_abi: &[u8]) -> Option<Arc<Abi>> {
        self.decoders.remove(&account.0);
        if packed_abi.is_empty() {
            self.abis.insert(account.0, None);
            return None;
        }
        match Abi::from_bin(packed_abi) {
            Ok(abi) => {
                let abi = Arc::new(abi);
                self.abis.insert(account.0, Some(abi.clone()));
                Some(abi)
            }
            Err(e) => {
                tracing::warn!(account = %account, error = %e, "failed to parse on-chain ABI");
                self.abis.insert(account.0, None);
                None
            }
        }
    }

    /// Cached ABI, falling back to the chain API on a miss.
    pub async fn get_or_fetch(&mut self, account: Name) -> Option<Arc<Abi>> {
        if let Some(entry) = self.abis.get(&account.0) {
            return entry.clone();
        }
        let fetched = self.chain.get_abi(account).await;
        if let Err(e) = &fetched {
            tracing::debug!(account = %account, error = %e, "get_abi fetch failed");
        }
        let entry = fetched.ok().flatten().map(Arc::new);
        self.abis.insert(account.0, entry.clone());
        entry
    }

    /// Reuse prepared type lookups until an on-chain ABI update invalidates them.
    pub async fn decoder(&mut self, account: Name) -> Option<Arc<AbiDecoder<'static>>> {
        if let Some(decoder) = self.decoders.get(&account.0) {
            return Some(decoder.clone());
        }
        let abi = self.get_or_fetch(account).await?;
        let decoder = Arc::new(AbiDecoder::from_shared(abi));
        self.decoders.insert(account.0, decoder.clone());
        Some(decoder)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn packed_abi(value_type: &str) -> Vec<u8> {
        fn string(bytes: &mut Vec<u8>, value: &str) {
            bytes.push(value.len() as u8);
            bytes.extend_from_slice(value.as_bytes());
        }
        let mut bytes = Vec::new();
        string(&mut bytes, "eosio::abi/1.1");
        bytes.push(1); // alias
        string(&mut bytes, "value_type");
        string(&mut bytes, value_type);
        bytes.push(1); // struct
        string(&mut bytes, "row");
        string(&mut bytes, "");
        bytes.push(1); // field
        string(&mut bytes, "value");
        string(&mut bytes, "value_type");
        bytes.push(1); // action
        bytes.extend_from_slice(&"run".parse::<Name>().unwrap().0.to_le_bytes());
        string(&mut bytes, "row");
        string(&mut bytes, "");
        bytes.push(1); // table
        bytes.extend_from_slice(&"rows".parse::<Name>().unwrap().0.to_le_bytes());
        string(&mut bytes, "i64");
        bytes.extend_from_slice(&[0, 0]); // key names/types
        string(&mut bytes, "row");
        bytes.extend_from_slice(&[0, 0, 0, 0]); // clauses, errors, extensions, variants
        bytes
    }

    #[tokio::test]
    async fn reuses_decoders_and_invalidates_on_updates_and_clear() {
        let mut cache = AbiCache::new(ChainClient::new("http://127.0.0.1:1", Default::default()));
        let account = "contract".parse().unwrap();
        let action = "run".parse().unwrap();
        let table = "rows".parse().unwrap();
        cache.update(account, &packed_abi("uint32")).unwrap();
        let first = cache.decoder(account).await.unwrap();
        assert!(Arc::ptr_eq(&first, &cache.decoder(account).await.unwrap()));
        assert_eq!(
            first.decode_action(action, &7u32.to_le_bytes()).unwrap()["value"],
            7
        );
        assert_eq!(
            first.decode_table_row(table, &8u32.to_le_bytes()).unwrap()["value"],
            8
        );

        cache.update(account, &packed_abi("uint64")).unwrap();
        let updated = cache.decoder(account).await.unwrap();
        assert!(!Arc::ptr_eq(&first, &updated));
        let large = u32::MAX as u64 + 1;
        assert_eq!(
            updated.decode_action(action, &large.to_le_bytes()).unwrap()["value"],
            large
        );
        assert!(updated.decode_action(action, &7u32.to_le_bytes()).is_err());

        cache.update(account, &[]);
        assert!(cache.decoder(account).await.is_none());
        cache.update(account, &packed_abi("uint32")).unwrap();
        assert!(cache.decoder(account).await.is_some());
        cache.update(account, &[255]);
        assert!(cache.decoder(account).await.is_none());
    }
}
