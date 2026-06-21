//! Content-hash module loading and hot deployment for routing functions.
//!
//! Status (2026-06): this is an in-memory `HashMap` keyed by content hash, not a
//! beamr module registry. Module bytecode is only hashed for dedup; it is never
//! executed — routing logic is supplied as a native Rust closure. Modules are
//! content-addressed: loading the same bytecode twice reuses the already-loaded
//! module, and hot deployment atomically swaps the active function while
//! in-flight executions keep their own reference to the previous version until
//! they complete.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use crate::routing::function::execute::{ConsumerStateView, RoutingDecision, RoutingMessage};

/// Native entrypoint a loaded routing module exposes to the supervisor.
pub(super) type RoutingLogic =
    dyn Fn(&RoutingMessage, &[ConsumerStateView]) -> RoutingDecision + Send + Sync + 'static;

/// Content hash identifying a routing function module.
///
/// Two modules with identical bytecode share the same content hash and are
/// loaded only once by [`ModuleLoader`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ContentHash(u64);

impl ContentHash {
    /// Computes the content hash of routing function bytecode.
    #[must_use]
    pub fn of(bytecode: &[u8]) -> Self {
        const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
        const PRIME: u64 = 0x0000_0100_0000_01b3;

        let mut hash = OFFSET;
        for byte in bytecode {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(PRIME);
        }
        Self(hash)
    }
}

impl std::fmt::Display for ContentHash {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{:016x}", self.0)
    }
}

/// A routing module submitted for loading.
///
/// Carries the module bytecode (used to derive its [`ContentHash`]) and the
/// native entrypoint the loaded module executes.
pub struct RoutingModule {
    content_hash: ContentHash,
    logic: Arc<RoutingLogic>,
}

impl RoutingModule {
    /// Creates a routing module from its bytecode and native entrypoint.
    #[must_use]
    pub fn new<F>(bytecode: &[u8], logic: F) -> Self
    where
        F: Fn(&RoutingMessage, &[ConsumerStateView]) -> RoutingDecision + Send + Sync + 'static,
    {
        Self {
            content_hash: ContentHash::of(bytecode),
            logic: Arc::new(logic),
        }
    }

    /// Returns the content hash of the module.
    #[must_use]
    pub const fn content_hash(&self) -> ContentHash {
        self.content_hash
    }
}

impl std::fmt::Debug for RoutingModule {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RoutingModule")
            .field("content_hash", &self.content_hash)
            .finish_non_exhaustive()
    }
}

struct LoadedModule {
    content_hash: ContentHash,
    logic: Arc<RoutingLogic>,
}

impl std::fmt::Debug for LoadedModule {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LoadedModule")
            .field("content_hash", &self.content_hash)
            .finish_non_exhaustive()
    }
}

/// An executable routing function referencing a loaded module.
#[derive(Clone)]
pub struct RoutingFunction {
    module: Arc<LoadedModule>,
}

impl RoutingFunction {
    /// Returns the content hash of the underlying module.
    #[must_use]
    pub fn content_hash(&self) -> ContentHash {
        self.module.content_hash
    }

    /// Returns a handle to the module's native entrypoint for the supervisor.
    pub(super) fn logic(&self) -> Arc<RoutingLogic> {
        Arc::clone(&self.module.logic)
    }
}

impl std::fmt::Debug for RoutingFunction {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RoutingFunction")
            .field("content_hash", &self.module.content_hash)
            .finish_non_exhaustive()
    }
}

/// Loads routing modules by content hash, deduplicating identical bytecode.
///
/// Backed by an in-memory `HashMap` keyed by content hash, not a beamr module
/// registry; the bytecode is hashed for dedup only and is never executed.
/// Loading the same content hash twice returns a handle to the already-loaded
/// module rather than loading it again.
#[derive(Debug, Default)]
pub struct ModuleLoader {
    loaded: Mutex<HashMap<ContentHash, Arc<LoadedModule>>>,
}

impl ModuleLoader {
    /// Creates an empty module loader.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Loads `module`, returning an executable routing function.
    ///
    /// If a module with the same content hash is already loaded, the existing
    /// module is reused and no duplicate is loaded.
    #[must_use]
    pub fn load(&self, module: RoutingModule) -> RoutingFunction {
        let loaded_module = {
            let mut loaded = lock(&self.loaded);
            Arc::clone(loaded.entry(module.content_hash).or_insert_with(|| {
                Arc::new(LoadedModule {
                    content_hash: module.content_hash,
                    logic: module.logic,
                })
            }))
        };
        RoutingFunction {
            module: loaded_module,
        }
    }

    /// Returns the number of distinct modules currently loaded.
    #[must_use]
    pub fn loaded_count(&self) -> usize {
        lock(&self.loaded).len()
    }

    /// Returns true when a module with `hash` is loaded.
    #[must_use]
    pub fn is_loaded(&self, hash: ContentHash) -> bool {
        lock(&self.loaded).contains_key(&hash)
    }
}

/// Holds the active routing function for a channel and supports hot deployment.
///
/// Deploying a new version atomically swaps the active reference. In-flight
/// executions hold their own clone of the previous function and complete
/// normally; the previous module stays loaded until those clones are dropped.
#[derive(Debug)]
pub struct RoutingSlot {
    current: Mutex<RoutingFunction>,
}

impl RoutingSlot {
    /// Creates a slot holding `initial` as the active routing function.
    #[must_use]
    pub const fn new(initial: RoutingFunction) -> Self {
        Self {
            current: Mutex::new(initial),
        }
    }

    /// Returns a handle to the currently active routing function.
    #[must_use]
    pub fn current(&self) -> RoutingFunction {
        lock(&self.current).clone()
    }

    /// Hot-deploys `next` as the active routing function.
    pub fn deploy(&self, next: RoutingFunction) {
        *lock(&self.current) = next;
    }

    /// Returns the content hash of the active routing function.
    #[must_use]
    pub fn active_hash(&self) -> ContentHash {
        lock(&self.current).content_hash()
    }
}

pub(super) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

#[cfg(test)]
mod tests {
    use super::{ContentHash, ModuleLoader, RoutingDecision, RoutingModule, RoutingSlot};

    fn noop_module(bytecode: &[u8]) -> RoutingModule {
        RoutingModule::new(bytecode, |_message, _consumers| RoutingDecision::none())
    }

    #[test]
    fn content_hash_is_stable_and_distinguishes_bytecode() {
        assert_eq!(ContentHash::of(b"module-a"), ContentHash::of(b"module-a"));
        assert_ne!(ContentHash::of(b"module-a"), ContentHash::of(b"module-b"));
    }

    #[test]
    fn load_returns_executable_function_keyed_by_content_hash() {
        let loader = ModuleLoader::new();
        let module = noop_module(b"v1");
        let hash = module.content_hash();

        let function = loader.load(module);

        assert_eq!(function.content_hash(), hash);
        assert!(loader.is_loaded(hash));
    }

    #[test]
    fn loading_same_content_hash_twice_does_not_duplicate() {
        let loader = ModuleLoader::new();

        let _first = loader.load(noop_module(b"v1"));
        let _second = loader.load(noop_module(b"v1"));

        assert_eq!(loader.loaded_count(), 1);
    }

    #[test]
    fn slot_deploy_swaps_active_function() {
        let loader = ModuleLoader::new();
        let old = loader.load(noop_module(b"v1"));
        let new = loader.load(noop_module(b"v2"));
        let new_hash = new.content_hash();

        let slot = RoutingSlot::new(old);
        slot.deploy(new);

        assert_eq!(slot.active_hash(), new_hash);
    }
}
