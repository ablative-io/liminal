use std::sync::Mutex;

use liminal::protocol::{CausalContext, MessageEnvelope, SchemaId};

use super::*;
use crate::server::connection::services::{ConversationResource, SubscriptionResource};

#[derive(Debug, Default)]
struct RecordingServices {
    publishes: Mutex<Vec<(String, Vec<u8>)>>,
    subscriptions: Mutex<Vec<(String, usize)>>,
    conversations: Mutex<Vec<(u64, String)>>,
}

impl ConnectionServices for RecordingServices {
    fn publish(&self, channel: &str, envelope: &MessageEnvelope) -> Result<u64, ServerError> {
        self.publishes
            .lock()
            .map_err(|error| ServerError::ListenerAccept {
                message: format!("test publish recorder unavailable: {error}"),
            })?
            .push((channel.to_owned(), envelope.payload.clone()));
        Ok(42)
    }

    fn subscribe(
        &self,
        channel: &str,
        accepted_schemas: &[ProtocolSchemaId],
    ) -> Result<ConnectionSubscription, ServerError> {
        self.subscriptions
            .lock()
            .map_err(|error| ServerError::ListenerAccept {
                message: format!("test subscription recorder unavailable: {error}"),
            })?
            .push((channel.to_owned(), accepted_schemas.len()));
        Ok(ConnectionSubscription::new(
            7,
            schema_id(),
            Box::new(TestSubscription),
        ))
    }

    fn unsubscribe(&self, subscription: ConnectionSubscription) -> Result<(), ServerError> {
        subscription.unsubscribe()
    }

    fn open_conversation(
        &self,
        conversation_id: u64,
        subject: &str,
    ) -> Result<ConnectionConversation, ServerError> {
        self.conversations
            .lock()
            .map_err(|error| ServerError::ListenerAccept {
                message: format!("test conversation recorder unavailable: {error}"),
            })?
            .push((conversation_id, subject.to_owned()));
        Ok(ConnectionConversation::new(Box::new(TestConversation)))
    }

    fn conversation_message(
        &self,
        conversation: &ConnectionConversation,
        envelope: &MessageEnvelope,
    ) -> Result<(), ServerError> {
        conversation.message(envelope)
    }

    fn close_conversation(&self, conversation: ConnectionConversation) -> Result<(), ServerError> {
        conversation.close()
    }
}

#[derive(Debug)]
struct TestSubscription;

impl SubscriptionResource for TestSubscription {
    fn unsubscribe(self: Box<Self>) -> Result<(), ServerError> {
        Ok(())
    }
}

#[derive(Debug)]
struct TestConversation;

impl ConversationResource for TestConversation {
    fn message(&self, envelope: &MessageEnvelope) -> Result<(), ServerError> {
        if envelope.payload.is_empty() {
            return Err(ServerError::ListenerAccept {
                message: "empty test payload".to_owned(),
            });
        }
        Ok(())
    }

    fn close(self: Box<Self>) -> Result<(), ServerError> {
        Ok(())
    }
}

#[test]
fn publish_frame_delegates_to_liminal_services() -> Result<(), ServerError> {
    let services = RecordingServices::default();
    let envelope = envelope(b"hello".to_vec());
    let frame = Frame::Publish {
        flags: 0,
        stream_id: 3,
        channel: "orders".to_owned(),
        envelope,
    };
    let mut state = ConnectionProcessState::default();

    let action = apply_frame(&services, &mut state, frame);

    assert!(matches!(
        action,
        FrameAction::Respond(Frame::PublishAck {
            stream_id: 3,
            message_id: 42,
            ..
        })
    ));
    let first_call = {
        let calls = services
            .publishes
            .lock()
            .map_err(|error| ServerError::ListenerAccept {
                message: format!("test publish recorder unavailable: {error}"),
            })?;
        assert_eq!(calls.len(), 1);
        calls[0].clone()
    };
    assert_eq!(first_call.0, "orders");
    assert_eq!(first_call.1, b"hello".to_vec());
    Ok(())
}

#[test]
fn subscribe_and_unsubscribe_delegate_to_services() -> Result<(), ServerError> {
    let services = RecordingServices::default();
    let mut state = ConnectionProcessState::default();
    let subscribe = Frame::Subscribe {
        flags: 0,
        stream_id: 1,
        channel: "orders".to_owned(),
        accepted_schemas: Vec::new(),
    };

    let action = apply_frame(&services, &mut state, subscribe);

    assert!(matches!(
        action,
        FrameAction::Respond(Frame::SubscribeAck {
            subscription_id: 7,
            ..
        })
    ));
    assert!(state.subscriptions.contains_key(&7));
    let unsubscribe = Frame::Unsubscribe {
        flags: 0,
        stream_id: 1,
        subscription_id: 7,
    };
    let action = apply_frame(&services, &mut state, unsubscribe);
    assert!(matches!(action, FrameAction::NoResponse));
    assert!(!state.subscriptions.contains_key(&7));
    let first_subscription = {
        let calls = services
            .subscriptions
            .lock()
            .map_err(|error| ServerError::ListenerAccept {
                message: format!("test subscription recorder unavailable: {error}"),
            })?;
        assert_eq!(calls.len(), 1);
        calls[0].clone()
    };
    assert_eq!(first_subscription, ("orders".to_owned(), 0));
    Ok(())
}

fn envelope(payload: Vec<u8>) -> MessageEnvelope {
    MessageEnvelope::new(schema_id(), CausalContext::independent(), payload)
}

fn schema_id() -> SchemaId {
    SchemaId::new([1; SchemaId::WIRE_LEN])
}
