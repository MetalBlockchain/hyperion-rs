//! Backend abstraction: support ES-only, CH-only, or dual-write modes.

use crate::clickhouse::{ClickHouse, ClickHouseBatch, DivergenceCounter};
use crate::elastic::Elastic;
use anyhow::Result;
use serde_json::Value;
use std::sync::Arc;

/// Index writer backend: Elasticsearch, ClickHouse, or both.
pub enum IndexBackend {
    /// Elasticsearch only (legacy, current production).
    Elasticsearch(Elastic),
    /// ClickHouse only (new production target).
    ClickHouse(ClickHouse),
    /// Both simultaneously with divergence tracking.
    DualWrite {
        es: Elastic,
        ck: ClickHouse,
        counter: Arc<DivergenceCounter>,
    },
}

impl IndexBackend {
    pub fn new_es(es: Elastic) -> Self {
        IndexBackend::Elasticsearch(es)
    }

    pub fn new_ck(ck: ClickHouse) -> Self {
        IndexBackend::ClickHouse(ck)
    }

    pub fn new_dual(es: Elastic, ck: ClickHouse) -> Self {
        IndexBackend::DualWrite {
            es,
            ck,
            counter: DivergenceCounter::new(),
        }
    }

    pub fn divergence_counter(&self) -> Option<&DivergenceCounter> {
        match self {
            IndexBackend::DualWrite { counter, .. } => Some(counter),
            _ => None,
        }
    }
}

/// Query backend: read from ES, CH, or both (shadow).
pub enum QueryBackend {
    Elasticsearch(Elastic),
    ClickHouse(ClickHouse),
    Shadow {
        primary: Box<QueryBackend>,
        secondary: Box<QueryBackend>,
    },
}

impl QueryBackend {
    pub fn new_es(es: Elastic) -> Self {
        QueryBackend::Elasticsearch(es)
    }

    pub fn new_ck(ck: ClickHouse) -> Self {
        QueryBackend::ClickHouse(ck)
    }

    pub fn new_shadow(primary: QueryBackend, secondary: QueryBackend) -> Self {
        QueryBackend::Shadow {
            primary: Box::new(primary),
            secondary: Box::new(secondary),
        }
    }
}
