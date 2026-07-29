//! Loud-refusal pins for the four success-reporting voids in `liminal-sdk`.
//!
//! Each void used to hand the caller a green result while nothing reached any
//! wire and nothing was ever delivered:
//!
//! * (a) the transport `RemoteConfig::new` installs encoded a frame, discarded
//!   it, and returned a synthesised `Accept`;
//! * (b) typed `RemoteChannelHandle::subscribe` returned a stream that was
//!   immediately and permanently dry, so `while let Some(m) = sub.next().await`
//!   exited at once while the server pumped `Deliver` frames;
//! * (c) the embedded backends `EmbeddedConfig::new` installs by default
//!   discarded the message and reported `Accept`;
//! * (d) typed `EmbeddedChannelHandle::subscribe` returned the same
//!   instantly-dry stream as (b), with no in-process delivery path behind it at
//!   all — the silent sibling left standing while (a), (b), and (c) were made
//!   to refuse.
//!
//! These pins assert the typed refusals that replaced them. They are the
//! oracle for "refuses loudly instead of succeeding at nothing" — the scope is
//! REFUSE, not build: none of them asserts that the absent feature works.

use core::pin::pin;
use core::task::{Context, Poll, Waker};

use futures_core::Stream;
use liminal_sdk::{
    ChannelHandle, ConnectionPoolConfig, ConversationHandle, EmbeddedChannelHandle, EmbeddedConfig,
    EmbeddedConversationHandle, RemoteChannelHandle, RemoteConfig, RemoteConversationHandle,
    SchemaMetadata, SchemaValidate, SdkError,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
struct VoidMessage {
    id: u64,
}

impl SchemaValidate for VoidMessage {
    fn schema_metadata() -> SchemaMetadata {
        SchemaMetadata::new("void.message", "1", br#"{"type":"object"}"#.as_slice())
    }
}

/// A remote configuration that was never handed a real transport — the exact
/// state `RemoteConfig::new` leaves a caller in.
fn never_connected() -> Result<RemoteConfig, SdkError> {
    RemoteConfig::new(
        "127.0.0.1:9000",
        "events",
        "conversation",
        ConnectionPoolConfig::new(1, 10, 16),
    )
}

fn refusal_expected(what: &str, got: &str) -> SdkError {
    SdkError::Protocol {
        description: format!("{what} must refuse loudly, got {got}"),
    }
}

// ---- void (a): the never-connected default transport ----

#[test]
fn unconnected_remote_publish_refuses_instead_of_synthesizing_accept() -> Result<(), SdkError> {
    let handle = RemoteChannelHandle::new(&never_connected()?)?;

    let result = handle.publish(VoidMessage { id: 1 });

    assert!(
        matches!(result, Err(SdkError::NotConnected { .. })),
        "publish on a never-connected transport must refuse, got {result:?}"
    );
    Ok(())
}

#[test]
fn unconnected_remote_publish_with_idempotency_key_refuses() -> Result<(), SdkError> {
    let handle = RemoteChannelHandle::new(&never_connected()?)?;

    let result = handle.publish_with_idempotency_key(&VoidMessage { id: 2 }, "dispatch-1");

    assert!(
        matches!(result, Err(SdkError::NotConnected { .. })),
        "a keyed publish on a never-connected transport must refuse, got {result:?}"
    );
    Ok(())
}

#[test]
fn unconnected_remote_conversation_send_refuses() -> Result<(), SdkError> {
    let handle = RemoteConversationHandle::new(&never_connected()?);

    let result = handle.send(VoidMessage { id: 3 });

    assert!(
        matches!(result, Err(SdkError::NotConnected { .. })),
        "a conversation send on a never-connected transport must refuse, got {result:?}"
    );
    Ok(())
}

#[test]
fn unconnected_remote_resume_refuses() -> Result<(), SdkError> {
    let handle = RemoteChannelHandle::new(&never_connected()?)?;
    let subscription_id = handle.track_subscription()?;
    handle.acknowledge(subscription_id, 7)?;
    handle.reconnect_started()?;

    // `connected()` computes resume requests and hands each to the transport.
    // The never-connected transport must refuse rather than report that a
    // Resume frame it discarded was sent.
    let result = handle.connected();

    assert!(
        matches!(result, Err(SdkError::NotConnected { .. })),
        "resume on a never-connected transport must refuse, got {result:?}"
    );
    Ok(())
}

// ---- void (b): typed subscribe returning an instantly-dry stream ----

#[test]
fn typed_remote_subscribe_refuses_instead_of_returning_a_dry_stream() -> Result<(), SdkError> {
    let handle = RemoteChannelHandle::new(&never_connected()?)?;

    let subscription = handle.subscribe::<VoidMessage>();
    let mut subscription = pin!(subscription);
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);

    match subscription.as_mut().poll_next(&mut context) {
        Poll::Ready(Some(Err(SdkError::Unwired {
            surface,
            alternative,
            ..
        }))) => {
            assert!(
                surface.contains("subscribe"),
                "the refusal must name the surface that refused, got {surface}"
            );
            assert!(
                alternative.contains("SubscriptionStream"),
                "the refusal must point at the working lower-level surface, got {alternative}"
            );
            Ok(())
        }
        Poll::Ready(None) => Err(refusal_expected(
            "typed subscribe",
            "an instantly-dry stream (Ready(None))",
        )),
        Poll::Ready(Some(Err(other))) => Err(refusal_expected(
            "typed subscribe",
            &format!("the wrong typed error: {other}"),
        )),
        Poll::Ready(Some(Ok(_))) => Err(refusal_expected(
            "typed subscribe",
            "a buffered message it cannot have received",
        )),
        Poll::Pending => Err(refusal_expected("typed subscribe", "a parked stream")),
    }
}

// ---- void (c): the default embedded backends (seam B1) ----

#[test]
fn direct_embedded_publish_refuses_and_names_the_b1_seam() -> Result<(), SdkError> {
    let config = EmbeddedConfig::new("events", "conversation");
    let handle = EmbeddedChannelHandle::new(&config);

    let result = handle.publish(VoidMessage { id: 4 });

    match &result {
        Err(SdkError::Unwired {
            seam, alternative, ..
        }) => {
            assert!(
                seam.contains("B1"),
                "the embedded refusal must name the B1 seam, got {seam}"
            );
            assert!(
                alternative.contains("with_channel_backend"),
                "the refusal must name the backend the caller can install, got {alternative}"
            );
            Ok(())
        }
        _ => Err(refusal_expected(
            "the default embedded channel backend",
            &format!("{result:?}"),
        )),
    }
}

#[test]
fn direct_embedded_conversation_send_refuses_and_names_the_b1_seam() -> Result<(), SdkError> {
    let config = EmbeddedConfig::new("events", "conversation");
    let handle = EmbeddedConversationHandle::new(&config);

    let result = handle.send(VoidMessage { id: 5 });

    match &result {
        Err(SdkError::Unwired {
            seam, alternative, ..
        }) => {
            assert!(
                seam.contains("B1"),
                "the embedded refusal must name the B1 seam, got {seam}"
            );
            assert!(
                alternative.contains("with_conversation_backend"),
                "the refusal must name the backend the caller can install, got {alternative}"
            );
            Ok(())
        }
        _ => Err(refusal_expected(
            "the default embedded conversation backend",
            &format!("{result:?}"),
        )),
    }
}

// ---- void (d): embedded typed subscribe returning an instantly-dry stream ----

/// The embedded twin of void (b), and the harder lie of the two: the remote
/// handle at least had a working lower-level surface to point at, while embedded
/// mode has no in-process delivery path behind `subscribe` at all.
/// `EmbeddedChannelBackend` declares `publish` and nothing else, so no backend a
/// caller installs can feed this stream — an empty stream here reads as
/// "subscribed, nothing published yet" forever.
#[test]
fn embedded_subscribe_refuses_instead_of_returning_a_dry_stream() -> Result<(), SdkError> {
    let config = EmbeddedConfig::new("events", "conversation");
    let handle = EmbeddedChannelHandle::new(&config);

    let subscription = handle.subscribe::<VoidMessage>();
    let mut subscription = pin!(subscription);
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);

    let first = subscription.as_mut().poll_next(&mut context);
    let second = subscription.as_mut().poll_next(&mut context);

    match first {
        Poll::Ready(Some(Err(SdkError::Unwired {
            surface,
            alternative,
            ..
        }))) => {
            assert!(
                surface.contains("subscribe"),
                "the refusal must name the surface that refused, got {surface}"
            );
            assert!(
                surface.contains("embedded"),
                "the refusal must name WHICH subscribe refused, got {surface}"
            );
            assert!(
                alternative.contains("SubscriptionStream"),
                "the refusal must point at a surface that genuinely delivers, got {alternative}"
            );
            assert!(
                !alternative.contains("with_channel_backend"),
                "EmbeddedChannelBackend has publish and no subscription surface, so an \
                 installed backend cannot deliver a subscription; the refusal must not send \
                 the caller there for delivery, got {alternative}"
            );
            assert!(
                matches!(&second, Poll::Ready(None)),
                "the refusal must be one item and then end, got {second:?} on the second poll"
            );
            Ok(())
        }
        Poll::Ready(None) => Err(refusal_expected(
            "embedded typed subscribe",
            "an instantly-dry stream (Ready(None))",
        )),
        Poll::Ready(Some(Err(other))) => Err(refusal_expected(
            "embedded typed subscribe",
            &format!("the wrong typed error: {other}"),
        )),
        Poll::Ready(Some(Ok(_))) => Err(refusal_expected(
            "embedded typed subscribe",
            "a buffered message it cannot have received",
        )),
        Poll::Pending => Err(refusal_expected("embedded typed subscribe", "a parked stream")),
    }
}
