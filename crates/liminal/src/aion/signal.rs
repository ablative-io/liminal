#![allow(clippy::module_name_repetitions)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};

use super::channels::{ChannelName, signal_channel};
use super::error::AionSurfaceError;
use super::types::SignalPayload;
use crate::channel::{ChannelConfig, ChannelHandle, ChannelMode, SubscriptionHandle};
use crate::conversation::ParticipantPid;

#[path = "signal/codec.rs"]
mod codec;
#[path = "signal/defaults.rs"]
mod defaults;
#[cfg(test)]
#[path = "signal/tests.rs"]
mod tests;
#[path = "signal/types.rs"]
mod types;

pub use types::{
    RecordedSignalDelivery, SignalChannel, SignalDeclaration, SignalDeliverer, SignalOperation,
    SignalOperationKind, SignalRecorder, SignalWorkflowConfig, WorkflowTerminalStatus,
};

use codec::{build_signal_schema, drain_delivery, encode_signal, validate_signal};
use defaults::{NoopSignalDeliverer, NoopSignalRecorder};

/// Dependencies and registry state used by workflow signal delivery.
#[derive(Clone)]
pub struct SignalContext {
    registry: Arc<Mutex<SignalRegistry>>,
    deliverer: Arc<dyn SignalDeliverer>,
    recorder: Arc<dyn SignalRecorder>,
}

impl SignalContext {
    /// Creates a signal context from explicit delivery and recording dependencies.
    #[must_use]
    pub fn new(deliverer: Arc<dyn SignalDeliverer>, recorder: Arc<dyn SignalRecorder>) -> Self {
        Self {
            registry: Arc::new(Mutex::new(SignalRegistry::default())),
            deliverer,
            recorder,
        }
    }

    /// Starts signal delivery for a workflow, creating or reusing its typed channel.
    ///
    /// # Errors
    ///
    /// Returns [`AionSurfaceError`] when the channel name is invalid or channel creation fails.
    pub fn start_workflow_signals(
        &self,
        config: SignalWorkflowConfig,
    ) -> Result<Option<SignalChannel>, AionSurfaceError> {
        let namespace = config.namespace;
        let workflow = config.workflow_id;
        let target_pid = config.workflow_pid;
        let declarations = config.declarations;
        let mode = config.mode;

        if declarations.is_empty() {
            return Ok(None);
        }

        let channel_name = signal_channel(&namespace, &workflow)?;
        if let Some(channel) = self.reuse_existing(&channel_name, target_pid)? {
            return Ok(Some(channel));
        }

        let schema = build_signal_schema(&channel_name, &declarations)?;
        let key = channel_name.as_str().to_owned();
        let handle = ChannelHandle::new(ChannelConfig::new(key, schema, mode));
        let subscription = handle
            .subscribe()
            .map_err(|error| lifecycle_failed(&channel_name, error))?;
        let session = SignalSession {
            channel_name: channel_name.clone(),
            workflow_id: workflow,
            workflow_pid: target_pid,
            handle,
            subscription,
            declarations,
            mode,
        };
        Ok(Some(self.insert_or_reuse(
            &channel_name,
            session,
            target_pid,
        )?))
    }

    /// Publishes a signal after declaration validation and delivers it to the workflow mailbox.
    ///
    /// # Errors
    ///
    /// Returns [`AionSurfaceError`] when validation fails, the workflow is not running, or delivery
    /// fails.
    pub fn publish_signal(
        &self,
        namespace: &str,
        workflow_id: &str,
        signal: SignalPayload,
    ) -> Result<(), AionSurfaceError> {
        let signal = (signal,);
        self.publish_signal_ref(namespace, workflow_id, &signal.0)
    }

    /// Tears down a workflow's signal channel after the workflow reaches a terminal status.
    ///
    /// # Errors
    ///
    /// Returns [`AionSurfaceError`] when the channel name is invalid or teardown fails.
    pub fn complete_workflow_signals(
        &self,
        namespace: &str,
        workflow_id: &str,
        status: WorkflowTerminalStatus,
    ) -> Result<(), AionSurfaceError> {
        let channel_name = signal_channel(namespace, workflow_id)?;
        let session = {
            let mut registry = self.lock_registry(&channel_name)?;
            registry
                .terminated
                .insert(channel_name.as_str().to_owned(), status);
            registry.active.remove(channel_name.as_str())
        };

        if let Some(session) = session {
            session
                .handle
                .close()
                .map_err(|error| lifecycle_failed(&channel_name, error))?;
        }
        Ok(())
    }

    /// Returns recorded durable signal deliveries without live channel interaction.
    ///
    /// # Errors
    ///
    /// Returns [`AionSurfaceError`] when the signal channel name is invalid or replay fails.
    pub fn replay_signal_deliveries(
        &self,
        namespace: &str,
        workflow_id: &str,
    ) -> Result<Vec<RecordedSignalDelivery>, AionSurfaceError> {
        let channel_name = signal_channel(namespace, workflow_id)?;
        self.recorder
            .replay_deliveries(channel_name.as_str(), workflow_id)
    }

    /// Reports whether an active signal channel exists for a workflow.
    ///
    /// # Errors
    ///
    /// Returns [`AionSurfaceError`] when the signal channel name is invalid or the registry fails.
    pub fn has_signal_channel(
        &self,
        namespace: &str,
        workflow_id: &str,
    ) -> Result<bool, AionSurfaceError> {
        let channel_name = signal_channel(namespace, workflow_id)?;
        let registry = self.lock_registry(&channel_name)?;
        Ok(registry.active.contains_key(channel_name.as_str()))
    }

    fn insert_or_reuse(
        &self,
        channel_name: &ChannelName,
        session: SignalSession,
        target_pid: ParticipantPid,
    ) -> Result<SignalChannel, AionSurfaceError> {
        let mut registry = self.lock_registry(channel_name)?;
        if let Some(channel) = reuse_session(&mut registry, channel_name.as_str(), target_pid) {
            registry.terminated.remove(channel_name.as_str());
            drop(registry);
            return Ok(channel);
        }

        let channel = session.to_channel();
        registry
            .active
            .insert(channel_name.as_str().to_owned(), session);
        registry.terminated.remove(channel_name.as_str());
        drop(registry);
        Ok(channel)
    }

    fn reuse_existing(
        &self,
        channel_name: &ChannelName,
        target_pid: ParticipantPid,
    ) -> Result<Option<SignalChannel>, AionSurfaceError> {
        let mut registry = self.lock_registry(channel_name)?;
        let channel = reuse_session(&mut registry, channel_name.as_str(), target_pid);
        if channel.is_some() {
            registry.terminated.remove(channel_name.as_str());
        }
        drop(registry);
        Ok(channel)
    }

    fn publish_signal_ref(
        &self,
        namespace: &str,
        workflow_id: &str,
        signal: &SignalPayload,
    ) -> Result<(), AionSurfaceError> {
        let channel_name = signal_channel(namespace, workflow_id)?;
        let session =
            self.active_session(&channel_name, workflow_id, signal.signal_name.as_str())?;
        validate_signal(&session, signal)?;
        let encoded = encode_signal(&channel_name, workflow_id, signal)?;

        session.handle.publish(encoded).map_err(|error| {
            delivery_failed(&channel_name, workflow_id, &signal.signal_name, error)
        })?;
        let delivered = drain_delivery(&session)?;
        self.deliverer
            .deliver(session.workflow_pid, delivered.clone())?;
        if session.mode == ChannelMode::Durable {
            self.recorder.record(SignalOperation::delivered(
                &session.channel_name,
                workflow_id,
                &delivered,
                session.mode,
            ))?;
        }
        Ok(())
    }

    fn active_session(
        &self,
        channel_name: &ChannelName,
        workflow_id: &str,
        signal_name: &str,
    ) -> Result<SignalSession, AionSurfaceError> {
        let registry = self.lock_registry(channel_name)?;
        registry
            .active
            .get(channel_name.as_str())
            .cloned()
            .ok_or_else(|| inactive_signal_error(&registry, channel_name, workflow_id, signal_name))
    }

    fn lock_registry(
        &self,
        channel_name: &ChannelName,
    ) -> Result<MutexGuard<'_, SignalRegistry>, AionSurfaceError> {
        self.registry
            .lock()
            .map_err(|error| lifecycle_failed(channel_name, error))
    }
}

impl Default for SignalContext {
    fn default() -> Self {
        Self::new(Arc::new(NoopSignalDeliverer), Arc::new(NoopSignalRecorder))
    }
}

impl std::fmt::Debug for SignalContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SignalContext")
            .finish_non_exhaustive()
    }
}

/// Starts signal delivery for a workflow using an explicit context.
///
/// # Errors
///
/// Returns [`AionSurfaceError`] when the channel name is invalid or channel creation fails.
pub fn start_workflow_signals(
    context: &SignalContext,
    config: SignalWorkflowConfig,
) -> Result<Option<SignalChannel>, AionSurfaceError> {
    context.start_workflow_signals(config)
}

/// Publishes a signal to a running workflow using an explicit context.
///
/// # Errors
///
/// Returns [`AionSurfaceError`] when validation fails, the workflow is not running, or delivery
/// fails.
pub fn publish_signal(
    context: &SignalContext,
    namespace: &str,
    workflow_id: &str,
    signal: SignalPayload,
) -> Result<(), AionSurfaceError> {
    context.publish_signal(namespace, workflow_id, signal)
}

/// Tears down a workflow's signal channel using an explicit context.
///
/// # Errors
///
/// Returns [`AionSurfaceError`] when the channel name is invalid or teardown fails.
pub fn complete_workflow_signals(
    context: &SignalContext,
    namespace: &str,
    workflow_id: &str,
    status: WorkflowTerminalStatus,
) -> Result<(), AionSurfaceError> {
    context.complete_workflow_signals(namespace, workflow_id, status)
}

/// Replays durable signal deliveries using an explicit context.
///
/// # Errors
///
/// Returns [`AionSurfaceError`] when the signal channel name is invalid or replay fails.
pub fn replay_signal_deliveries(
    context: &SignalContext,
    namespace: &str,
    workflow_id: &str,
) -> Result<Vec<RecordedSignalDelivery>, AionSurfaceError> {
    context.replay_signal_deliveries(namespace, workflow_id)
}

#[derive(Clone, Debug)]
pub(super) struct SignalSession {
    channel_name: ChannelName,
    workflow_id: String,
    workflow_pid: ParticipantPid,
    handle: ChannelHandle,
    subscription: SubscriptionHandle,
    declarations: Vec<SignalDeclaration>,
    mode: ChannelMode,
}

impl SignalSession {
    fn to_channel(&self) -> SignalChannel {
        SignalChannel {
            channel_name: self.channel_name.clone(),
            handle: self.handle.clone(),
            declarations: self.declarations.clone(),
            mode: self.mode,
        }
    }
}

#[derive(Debug, Default)]
struct SignalRegistry {
    active: BTreeMap<String, SignalSession>,
    terminated: BTreeMap<String, WorkflowTerminalStatus>,
}

fn reuse_session(
    registry: &mut SignalRegistry,
    key: &str,
    target_pid: ParticipantPid,
) -> Option<SignalChannel> {
    registry.active.get_mut(key).map(|session| {
        session.workflow_pid = target_pid;
        session.to_channel()
    })
}

fn inactive_signal_error(
    registry: &SignalRegistry,
    channel_name: &ChannelName,
    workflow_id: &str,
    signal_name: &str,
) -> AionSurfaceError {
    let message = registry.terminated.get(channel_name.as_str()).map_or_else(
        || "workflow is not running or has no signal channel".to_owned(),
        |status| format!("workflow is terminated with status {}", status.as_str()),
    );
    delivery_failed(channel_name, workflow_id, signal_name, message)
}

fn validation_failed(
    channel_name: &ChannelName,
    workflow_id: &str,
    signal: &SignalPayload,
    expected: &str,
    detail: impl std::fmt::Display,
) -> AionSurfaceError {
    AionSurfaceError::SignalValidationFailed {
        channel_name: String::from(channel_name.clone()),
        workflow_id: workflow_id.to_owned(),
        signal_name: signal.signal_name.clone(),
        message: format!(
            "{detail}; expected content type(s): {expected}; actual content type: {}",
            signal.payload.content_type
        ),
    }
}

fn delivery_failed(
    channel_name: &ChannelName,
    workflow_id: &str,
    signal_name: &str,
    message: impl std::fmt::Display,
) -> AionSurfaceError {
    AionSurfaceError::SignalDeliveryFailed {
        channel_name: String::from(channel_name.clone()),
        workflow_id: workflow_id.to_owned(),
        signal_name: signal_name.to_owned(),
        message: message.to_string(),
    }
}

fn lifecycle_failed(
    channel_name: &ChannelName,
    message: impl std::fmt::Display,
) -> AionSurfaceError {
    AionSurfaceError::ChannelLifecycleError {
        channel_name: String::from(channel_name.clone()),
        message: message.to_string(),
    }
}
