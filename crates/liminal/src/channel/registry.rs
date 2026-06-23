//! LIM-002 R6: the channel registry.
//!
//! [`ChannelRegistry`] maps channel names to their supervised actor handles on a
//! single shared [`ChannelSupervisor`]. It supports `create` (spawn a new
//! channel actor), `lookup` (by name), `list` (all active channels with their
//! current subscriber counts), and `close` (signal an actor to shut down and
//! drop its registry entry). Duplicate names are rejected.

use std::collections::HashMap;
use std::sync::Mutex;

use crate::channel::supervisor::ChannelSupervisor;
use crate::channel::types::{ChannelConfig, ChannelHandle};
use crate::error::LiminalError;

/// One active channel's name and current subscriber count.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChannelSummary {
    /// The channel's registered name.
    pub name: String,
    /// Number of currently-linked subscribers on the channel actor.
    pub subscriber_count: usize,
}

/// A name-keyed registry of supervised channel actors.
#[derive(Debug)]
pub struct ChannelRegistry {
    supervisor: ChannelSupervisor,
    channels: Mutex<HashMap<String, ChannelHandle>>,
}

impl ChannelRegistry {
    /// Builds a registry backed by a dedicated supervisor (its own scheduler).
    ///
    /// # Errors
    /// Returns [`LiminalError`] when the supervisor's scheduler cannot start.
    pub fn new() -> Result<Self, LiminalError> {
        Ok(Self {
            supervisor: ChannelSupervisor::new()?,
            channels: Mutex::new(HashMap::new()),
        })
    }

    /// Builds a registry over an existing supervisor.
    #[must_use]
    pub fn with_supervisor(supervisor: ChannelSupervisor) -> Self {
        Self {
            supervisor,
            channels: Mutex::new(HashMap::new()),
        }
    }

    /// Creates and registers a new channel actor for `config`, returning its
    /// handle. Rejects a name already present in the registry.
    ///
    /// # Errors
    /// Returns [`LiminalError::PublishFailed`] when `config.name` is already
    /// registered, or a [`LiminalError`] when the registry lock is poisoned.
    pub fn create(&self, config: ChannelConfig) -> Result<ChannelHandle, LiminalError> {
        let mut channels = self.lock()?;
        if channels.contains_key(&config.name) {
            return Err(LiminalError::PublishFailed {
                message: format!("channel '{}' already exists", config.name),
            });
        }
        let name = config.name.clone();
        let handle = ChannelHandle::with_supervisor(config, self.supervisor.clone());
        channels.insert(name, handle.clone());
        drop(channels);
        Ok(handle)
    }

    /// Looks up a registered channel handle by name.
    ///
    /// # Errors
    /// Returns [`LiminalError`] when the registry lock is poisoned.
    pub fn lookup(&self, name: &str) -> Result<Option<ChannelHandle>, LiminalError> {
        Ok(self.lock()?.get(name).cloned())
    }

    /// Lists every active channel with its current subscriber count.
    ///
    /// The subscriber count is read live from each actor (`ListSubscribers`); a
    /// channel whose actor is unreachable reports a zero count rather than
    /// failing the whole listing.
    ///
    /// # Errors
    /// Returns [`LiminalError`] when the registry lock is poisoned.
    pub fn list(&self) -> Result<Vec<ChannelSummary>, LiminalError> {
        let snapshot: Vec<(String, ChannelHandle)> = {
            let channels = self.lock()?;
            channels
                .iter()
                .map(|(name, handle)| (name.clone(), handle.clone()))
                .collect()
        };
        let mut summaries = snapshot
            .into_iter()
            .map(|(name, handle)| ChannelSummary {
                subscriber_count: handle.subscriber_count().unwrap_or(0),
                name,
            })
            .collect::<Vec<_>>();
        summaries.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(summaries)
    }

    /// Closes the named channel (stopping its actor process) and drops its
    /// registry entry. Returns whether a channel was present.
    ///
    /// # Errors
    /// Returns [`LiminalError`] when the registry lock is poisoned or the close
    /// command fails.
    pub fn close(&self, name: &str) -> Result<bool, LiminalError> {
        let handle = self.lock()?.remove(name);
        match handle {
            Some(handle) => {
                handle.close()?;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Stops the underlying supervisor scheduler.
    pub fn shutdown(&self) {
        self.supervisor.shutdown();
    }

    fn lock(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, HashMap<String, ChannelHandle>>, LiminalError> {
        self.channels
            .lock()
            .map_err(|error| LiminalError::PublishFailed {
                message: format!("channel registry lock poisoned: {error}"),
            })
    }
}
