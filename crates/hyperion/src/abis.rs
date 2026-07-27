//! Contract ABI cache.
//!
//! During linear indexing, ABI updates arrive in block order via the
//! state-history `account` table deltas, so the cache always reflects the
//! chain state at the block being processed. On a cache miss (e.g. the
//! indexer started mid-chain) the current ABI is fetched from the chain API
//! — a pragmatic fallback that can mismatch very old blocks.

use crate::chain_client::ChainClient;
use antelope::{Abi, Name};
use std::collections::HashMap;
use std::sync::Arc;

pub struct AbiCache {
    chain: ChainClient,
    /// `None` means "known to have no (usable) ABI" — negative cache.
    abis: HashMap<u64, Option<Arc<Abi>>>,
}

impl AbiCache {
    pub fn new(chain: ChainClient) -> Self {
        AbiCache {
            chain,
            abis: HashMap::new(),
        }
    }

    /// Record an ABI update seen on-chain (packed `abi_def` bytes; empty
    /// bytes mean the ABI was cleared).
    pub fn update(&mut self, account: Name, packed_abi: &[u8]) -> Option<Arc<Abi>> {
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
}
