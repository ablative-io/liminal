use std::fmt;

use super::error::AionSurfaceError;

/// A validated Aion channel name following `aion.{kind}.{namespace}.{id}`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ChannelName(String);

impl ChannelName {
    /// Borrow the validated channel name as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ChannelName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl From<ChannelName> for String {
    fn from(channel_name: ChannelName) -> Self {
        channel_name.0
    }
}

/// Construct an activity dispatch channel name for a namespace and task queue.
///
/// # Errors
///
/// Returns [`AionSurfaceError::InvalidChannelName`] when either part is empty or contains a dot.
pub fn dispatch_channel(
    namespace: &str,
    task_queue: &str,
) -> Result<ChannelName, AionSurfaceError> {
    channel_name("dispatch", namespace, "task_queue", task_queue)
}

/// Construct a signal channel name for a namespace and workflow id.
///
/// # Errors
///
/// Returns [`AionSurfaceError::InvalidChannelName`] when either part is empty or contains a dot.
pub fn signal_channel(namespace: &str, workflow_id: &str) -> Result<ChannelName, AionSurfaceError> {
    channel_name("signal", namespace, "workflow_id", workflow_id)
}

/// Construct a history channel name for a namespace and workflow id.
///
/// # Errors
///
/// Returns [`AionSurfaceError::InvalidChannelName`] when either part is empty or contains a dot.
pub fn history_channel(
    namespace: &str,
    workflow_id: &str,
) -> Result<ChannelName, AionSurfaceError> {
    channel_name("history", namespace, "workflow_id", workflow_id)
}

fn channel_name(
    kind: &str,
    namespace: &str,
    id_part: &str,
    id: &str,
) -> Result<ChannelName, AionSurfaceError> {
    validate_part("namespace", namespace)?;
    validate_part(id_part, id)?;

    Ok(ChannelName(format!("aion.{kind}.{namespace}.{id}")))
}

fn validate_part(part: &str, input: &str) -> Result<(), AionSurfaceError> {
    if input.is_empty() {
        return Err(invalid_channel_name(part, input, "must not be empty"));
    }

    if input.contains('.') {
        return Err(invalid_channel_name(part, input, "must not contain dots"));
    }

    Ok(())
}

fn invalid_channel_name(part: &str, input: &str, message: &str) -> AionSurfaceError {
    AionSurfaceError::InvalidChannelName {
        part: part.to_owned(),
        input: input.to_owned(),
        message: message.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::{AionSurfaceError, dispatch_channel, history_channel, signal_channel};

    #[test]
    fn dispatch_channel_uses_aion_naming_convention() {
        let channel = dispatch_channel("default", "email-queue");

        assert_eq!(
            channel.as_ref().map(super::ChannelName::as_str),
            Ok("aion.dispatch.default.email-queue")
        );
    }

    #[test]
    fn signal_channel_uses_aion_naming_convention() {
        let channel = signal_channel("prod", "wf-123");

        assert_eq!(
            channel.as_ref().map(super::ChannelName::as_str),
            Ok("aion.signal.prod.wf-123")
        );
    }

    #[test]
    fn history_channel_uses_aion_naming_convention() {
        let channel = history_channel("prod", "wf-123");

        assert_eq!(
            channel.as_ref().map(super::ChannelName::as_str),
            Ok("aion.history.prod.wf-123")
        );
    }

    #[test]
    fn rejects_dot_in_namespace() {
        let channel = dispatch_channel("bad.ns", "q");

        assert!(matches!(
            channel,
            Err(AionSurfaceError::InvalidChannelName { .. })
        ));
    }

    #[test]
    fn rejects_empty_namespace() {
        let channel = signal_channel("", "wf-1");

        assert!(matches!(
            channel,
            Err(AionSurfaceError::InvalidChannelName { .. })
        ));
    }

    #[test]
    fn rejects_empty_workflow_id() {
        let channel = history_channel("ns", "");

        assert!(matches!(
            channel,
            Err(AionSurfaceError::InvalidChannelName { .. })
        ));
    }
}
