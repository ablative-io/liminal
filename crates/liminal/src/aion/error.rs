/// Error taxonomy for Aion's liminal integration surface.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum AionSurfaceError {
    /// Selecting a worker or completing the dispatch conversation failed.
    #[error("dispatch failed for workflow '{workflow_id}' on channel '{channel_name}': {message}")]
    DispatchFailed {
        /// Channel used for the dispatch conversation.
        channel_name: String,
        /// Workflow that requested the activity dispatch.
        workflow_id: String,
        /// Human-readable failure detail.
        message: String,
    },

    /// A linked worker process exited while handling work.
    #[error(
        "worker '{worker_id}' crashed while serving workflow '{workflow_id}' on channel '{channel_name}': {message}"
    )]
    WorkerCrashed {
        /// Channel whose linked worker exited.
        channel_name: String,
        /// Workflow affected by the worker exit.
        workflow_id: String,
        /// Linked worker process identifier.
        worker_id: String,
        /// Human-readable exit detail.
        message: String,
    },

    /// Publishing to a workflow signal channel failed.
    #[error(
        "signal delivery failed for signal '{signal_name}' to workflow '{workflow_id}' on channel '{channel_name}': {message}"
    )]
    SignalDeliveryFailed {
        /// Signal channel that rejected delivery.
        channel_name: String,
        /// Workflow targeted by the signal.
        workflow_id: String,
        /// Signal name being delivered.
        signal_name: String,
        /// Human-readable failure detail.
        message: String,
    },

    /// A signal payload did not match the workflow's declared signal type.
    #[error(
        "signal validation failed for signal '{signal_name}' to workflow '{workflow_id}' on channel '{channel_name}': {message}"
    )]
    SignalValidationFailed {
        /// Signal channel that performed validation.
        channel_name: String,
        /// Workflow targeted by the signal.
        workflow_id: String,
        /// Signal name whose payload failed validation.
        signal_name: String,
        /// Human-readable validation detail.
        message: String,
    },

    /// Publishing or subscribing to workflow history failed.
    #[error(
        "history streaming failed for workflow '{workflow_id}' on channel '{channel_name}': {message}"
    )]
    StreamingFailed {
        /// History channel being published to or subscribed from.
        channel_name: String,
        /// Workflow whose history stream failed.
        workflow_id: String,
        /// Human-readable failure detail.
        message: String,
    },

    /// A namespace, workflow id, or task queue input cannot form an Aion channel name.
    #[error("invalid channel name input '{input}' for {part}: {message}")]
    InvalidChannelName {
        /// Name component that failed validation.
        part: String,
        /// Rejected input value.
        input: String,
        /// Human-readable validation detail.
        message: String,
    },

    /// Creating or tearing down an Aion channel failed.
    #[error("channel lifecycle error for channel '{channel_name}': {message}")]
    ChannelLifecycleError {
        /// Channel being created or torn down.
        channel_name: String,
        /// Human-readable failure detail.
        message: String,
    },
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::super::{dispatch_channel, history_channel, signal_channel};
    use super::AionSurfaceError;

    fn assert_error_trait<E: Error>(_: &E) {}

    #[test]
    fn implements_std_error_and_debug() -> Result<(), AionSurfaceError> {
        let channel_name = String::from(history_channel("prod", "wf-123")?);
        let error = AionSurfaceError::ChannelLifecycleError {
            channel_name,
            message: "teardown failed".to_owned(),
        };
        let debug_output = format!("{error:?}");

        assert_error_trait(&error);
        assert!(error.source().is_none());
        assert!(!debug_output.is_empty());

        Ok(())
    }

    #[test]
    fn dispatch_display_includes_channel_and_workflow_context() -> Result<(), AionSurfaceError> {
        let channel_name = String::from(dispatch_channel("prod", "email-queue")?);
        let error = AionSurfaceError::DispatchFailed {
            channel_name: channel_name.clone(),
            workflow_id: "wf-123".to_owned(),
            message: "conversation failed".to_owned(),
        };
        let display = error.to_string();

        assert!(display.contains(&channel_name));
        assert!(display.contains("wf-123"));
        assert!(display.contains("conversation failed"));

        Ok(())
    }

    #[test]
    fn worker_crash_display_includes_channel_workflow_and_worker_context()
    -> Result<(), AionSurfaceError> {
        let channel_name = String::from(dispatch_channel("prod", "email-queue")?);
        let error = AionSurfaceError::WorkerCrashed {
            channel_name: channel_name.clone(),
            workflow_id: "wf-123".to_owned(),
            worker_id: "worker-9".to_owned(),
            message: "linked process exited".to_owned(),
        };
        let display = error.to_string();

        assert!(display.contains(&channel_name));
        assert!(display.contains("wf-123"));
        assert!(display.contains("worker-9"));
        assert!(display.contains("linked process exited"));

        Ok(())
    }

    #[test]
    fn signal_display_includes_channel_workflow_and_signal_context() -> Result<(), AionSurfaceError>
    {
        let channel_name = String::from(signal_channel("prod", "wf-123")?);
        let delivery = AionSurfaceError::SignalDeliveryFailed {
            channel_name: channel_name.clone(),
            workflow_id: "wf-123".to_owned(),
            signal_name: "approve".to_owned(),
            message: "publish failed".to_owned(),
        };
        let validation = AionSurfaceError::SignalValidationFailed {
            channel_name: channel_name.clone(),
            workflow_id: "wf-123".to_owned(),
            signal_name: "approve".to_owned(),
            message: "payload schema mismatch".to_owned(),
        };

        for display in [delivery.to_string(), validation.to_string()] {
            assert!(display.contains(&channel_name));
            assert!(display.contains("wf-123"));
            assert!(display.contains("approve"));
        }

        Ok(())
    }

    #[test]
    fn streaming_display_includes_channel_and_workflow_context() -> Result<(), AionSurfaceError> {
        let channel_name = String::from(history_channel("prod", "wf-123")?);
        let error = AionSurfaceError::StreamingFailed {
            channel_name: channel_name.clone(),
            workflow_id: "wf-123".to_owned(),
            message: "subscribe failed".to_owned(),
        };
        let display = error.to_string();

        assert!(display.contains(&channel_name));
        assert!(display.contains("wf-123"));
        assert!(display.contains("subscribe failed"));

        Ok(())
    }

    #[test]
    fn invalid_channel_name_display_identifies_invalid_input() {
        let error = AionSurfaceError::InvalidChannelName {
            part: "namespace".to_owned(),
            input: "bad.ns".to_owned(),
            message: "must not contain dots".to_owned(),
        };
        let display = error.to_string();

        assert!(display.contains("namespace"));
        assert!(display.contains("bad.ns"));
        assert!(display.contains("must not contain dots"));
    }
}
