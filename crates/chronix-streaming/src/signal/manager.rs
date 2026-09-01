//! Unified trigger management — keeps `TriggerEngine` and
//! `TriggerCatalog` in sync so that register/unregister/enable/disable
//! operations cannot diverge.

use std::sync::Arc;

use tracing::info;

use crate::signal::engine::TriggerEngine;
use crate::signal::error::Result;
use crate::signal::model::EventTrigger;
use crate::signal::sql::TriggerCatalog;

/// Facade that atomically updates both the runtime [`TriggerEngine`] and
/// the persistent [`TriggerCatalog`] on every mutation.
///
/// All public methods that modify trigger state update both stores.
/// Read-only methods delegate to the appropriate backing store.
pub struct TriggerManager {
    engine: Arc<TriggerEngine>,
    catalog: Arc<TriggerCatalog>,
}

impl TriggerManager {
    /// Create a new trigger manager.
    pub fn new(engine: Arc<TriggerEngine>, catalog: Arc<TriggerCatalog>) -> Self {
        Self { engine, catalog }
    }

    /// Register a trigger in both the engine and the catalog.
    ///
    /// Catalog-first ordering — the persistent catalog is
    /// updated before the volatile engine so that a crash between the
    /// two steps is recovered by `restore_from_catalog()` on restart.
    /// If engine registration fails (e.g. duplicate), the catalog entry
    /// is rolled back.
    pub fn register(&self, trigger: EventTrigger, sql: impl Into<String>) -> Result<()> {
        let id = trigger.id.clone();
        let sql = sql.into();

        // Track whether this is a new catalog entry (for rollback).
        let existed_before = self.catalog.len();

        // 1. Persist to catalog first (crash-safe, survives restart).
        self.catalog.insert(&id, &sql);
        let is_new = self.catalog.len() > existed_before;

        // 2. Register in engine — if this fails, roll back the catalog
        //    only if the entry was newly inserted (not an overwrite).
        if let Err(e) = self.engine.register(trigger) {
            if is_new {
                self.catalog.remove(&id);
            }
            return Err(e);
        }

        info!(trigger_id = %id, "trigger registered in engine + catalog");
        Ok(())
    }

    /// Unregister a trigger from both the engine and the catalog.
    ///
    /// Engine-first ordering — the trigger stops firing
    /// immediately. If the process crashes before the catalog is updated,
    /// `restore_from_catalog()` will safely re-add it on restart rather
    /// than losing data.
    pub fn unregister(&self, trigger_id: &str) -> Result<()> {
        // 1. Remove from engine first (stop firing immediately).
        self.engine.unregister(trigger_id)?;

        // 2. Remove from persistent catalog.
        self.catalog.remove(trigger_id);

        info!(trigger_id = %trigger_id, "trigger unregistered from engine + catalog");
        Ok(())
    }

    /// Enable or disable a trigger and sync the state to the catalog.
    pub fn set_enabled(&self, trigger_id: &str, enabled: bool) -> Result<()> {
        self.engine.set_enabled(trigger_id, enabled)?;
        self.catalog.set_enabled(trigger_id, enabled);

        info!(trigger_id = %trigger_id, enabled, "trigger enabled state updated");
        Ok(())
    }

    /// Restore triggers from the catalog into the engine.
    ///
    /// Parses each catalog entry's SQL and registers the resulting trigger
    /// in the engine via `execute_trigger_sql`. Entries that fail
    /// to parse or register are logged and skipped. The enabled state
    /// stored in the catalog entry is applied after registration.
    pub fn restore_from_catalog(&self) -> Result<usize> {
        let entries = self.catalog.entries();
        let mut restored = 0usize;

        for entry in &entries {
            match crate::signal::sql::parse_trigger_sql(&entry.sql) {
                Ok(stmt) => {
                    match crate::signal::sql::execute_trigger_sql(&self.engine, &stmt) {
                        Ok(_) => {
                            // Sync enabled state from catalog
                            if !entry.enabled {
                                if let Err(e) = self.engine.set_enabled(&entry.name, false) {
                                    tracing::warn!(
                                        trigger = %entry.name,
                                        error = %e,
                                        "failed to sync trigger enabled state"
                                    );
                                }
                            }
                            restored += 1;
                        }
                        Err(e) => {
                            tracing::warn!(
                                trigger = %entry.name,
                                error = %e,
                                "failed to restore trigger from catalog"
                            );
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        trigger = %entry.name,
                        error = %e,
                        "failed to parse trigger SQL from catalog"
                    );
                }
            }
        }

        info!(
            count = restored,
            total = entries.len(),
            "restored triggers from catalog"
        );
        Ok(restored)
    }

    /// Get references to the underlying engine and catalog.
    pub fn engine(&self) -> &Arc<TriggerEngine> {
        &self.engine
    }

    /// Get the catalog.
    pub fn catalog(&self) -> &Arc<TriggerCatalog> {
        &self.catalog
    }

    /// List all trigger IDs from the engine.
    pub fn trigger_ids(&self) -> Vec<String> {
        self.engine.trigger_ids()
    }

    /// Get a trigger by ID from the engine.
    pub fn get_trigger(&self, trigger_id: &str) -> Option<EventTrigger> {
        self.engine.get_trigger(trigger_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::signal::model::{ThresholdOp, TriggerCondition};

    fn make_trigger(id: &str, measurement: &str) -> EventTrigger {
        EventTrigger::new(
            id,
            id,
            measurement,
            TriggerCondition::FieldThreshold {
                field: "value".into(),
                op: ThresholdOp::Gt,
                value: 100.0,
            },
        )
    }

    #[test]
    fn register_syncs_engine_and_catalog() {
        let engine = Arc::new(TriggerEngine::new());
        let catalog = Arc::new(TriggerCatalog::new());
        let mgr = TriggerManager::new(engine.clone(), catalog.clone());

        let trigger = make_trigger("t1", "cpu");
        mgr.register(trigger, "CREATE TRIGGER t1 ...").unwrap();

        assert!(engine.get_trigger("t1").is_some());
        assert_eq!(catalog.len(), 1);
    }

    #[test]
    fn unregister_syncs_engine_and_catalog() {
        let engine = Arc::new(TriggerEngine::new());
        let catalog = Arc::new(TriggerCatalog::new());
        let mgr = TriggerManager::new(engine.clone(), catalog.clone());

        let trigger = make_trigger("t1", "cpu");
        mgr.register(trigger, "CREATE TRIGGER t1 ...").unwrap();
        mgr.unregister("t1").unwrap();

        assert!(engine.get_trigger("t1").is_none());
        assert_eq!(catalog.len(), 0);
    }

    #[test]
    fn set_enabled_syncs() {
        let engine = Arc::new(TriggerEngine::new());
        let catalog = Arc::new(TriggerCatalog::new());
        let mgr = TriggerManager::new(engine.clone(), catalog.clone());

        let trigger = make_trigger("t1", "cpu");
        mgr.register(trigger, "CREATE TRIGGER t1 ...").unwrap();
        mgr.set_enabled("t1", false).unwrap();

        let t = engine.get_trigger("t1").unwrap();
        assert!(!t.enabled);
        // Catalog entry should also be disabled
        let entries = catalog.entries();
        assert!(!entries[0].enabled);
    }

    #[test]
    fn duplicate_register_fails() {
        let engine = Arc::new(TriggerEngine::new());
        let catalog = Arc::new(TriggerCatalog::new());
        let mgr = TriggerManager::new(engine, catalog.clone());

        let trigger = make_trigger("t1", "cpu");
        mgr.register(trigger.clone(), "SQL").unwrap();
        assert!(mgr.register(trigger, "SQL").is_err());
        // Catalog should only have one entry
        assert_eq!(catalog.len(), 1);
    }
}
