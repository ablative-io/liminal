use alloc::sync::Arc;
use spin::Mutex;

use crate::SdkError;

use super::{
    ConnectionEvent, ConnectionLifecycle, ConnectionState, DisconnectReason, ReconnectAttempt,
    ReconnectEvent,
};

#[test]
fn disconnected_to_connected_is_rejected() -> Result<(), SdkError> {
    let mut lifecycle = ConnectionLifecycle::default();
    lifecycle.disconnect(DisconnectReason::Normal)?;

    assert!(lifecycle.connected().is_err());

    lifecycle.connect()?;
    lifecycle.connected()?;
    assert_eq!(lifecycle.state(), &ConnectionState::Connected);
    Ok(())
}

#[test]
fn observers_receive_successful_transitions() -> Result<(), SdkError> {
    let events = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&events);
    let mut lifecycle = ConnectionLifecycle::default();

    lifecycle.observe(move |event| observed.lock().push(event.clone()));
    lifecycle.connected()?;
    lifecycle.disconnect(DisconnectReason::Timeout)?;

    assert_eq!(events.lock().len(), 2);
    let first = events.lock()[0].clone();
    assert_eq!(
        first,
        ConnectionEvent::new(ConnectionState::Connecting, ConnectionState::Connected)
    );
    Ok(())
}

#[test]
fn disconnect_from_connecting_is_observable() -> Result<(), SdkError> {
    let events = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&events);
    let mut lifecycle = ConnectionLifecycle::default();

    // A fresh lifecycle starts in `Connecting`.
    assert_eq!(lifecycle.state(), &ConnectionState::Connecting);

    lifecycle.observe(move |event| observed.lock().push(event.clone()));
    lifecycle.disconnect(DisconnectReason::Error)?;

    assert_eq!(
        lifecycle.state(),
        &ConnectionState::Disconnected {
            reason: DisconnectReason::Error
        }
    );
    assert_eq!(events.lock().len(), 1);
    let observed_event = events.lock()[0].clone();
    assert_eq!(
        observed_event,
        ConnectionEvent::new(
            ConnectionState::Connecting,
            ConnectionState::Disconnected {
                reason: DisconnectReason::Error
            }
        )
    );
    Ok(())
}

#[test]
fn disconnect_from_reconnecting_is_observable() -> Result<(), SdkError> {
    let events = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&events);
    let mut lifecycle = ConnectionLifecycle::default();

    lifecycle.connected()?;
    lifecycle.reconnect(ReconnectEvent::EstablishedConnectionFate)?;
    assert_eq!(
        lifecycle.state(),
        &ConnectionState::Reconnecting { attempt: 0 }
    );

    lifecycle.observe(move |event| observed.lock().push(event.clone()));
    lifecycle.disconnect(DisconnectReason::Error)?;

    assert_eq!(
        lifecycle.state(),
        &ConnectionState::Disconnected {
            reason: DisconnectReason::Error
        }
    );
    assert_eq!(events.lock().len(), 1);
    let observed_event = events.lock()[0].clone();
    assert_eq!(
        observed_event,
        ConnectionEvent::new(
            ConnectionState::Reconnecting { attempt: 0 },
            ConnectionState::Disconnected {
                reason: DisconnectReason::Error
            }
        )
    );
    Ok(())
}

#[test]
fn disconnect_from_disconnected_is_rejected() -> Result<(), SdkError> {
    let mut lifecycle = ConnectionLifecycle::default();

    lifecycle.disconnect(DisconnectReason::Normal)?;
    assert_eq!(
        lifecycle.state(),
        &ConnectionState::Disconnected {
            reason: DisconnectReason::Normal
        }
    );

    // Disconnecting from the disconnected state is an illegal transition.
    assert!(lifecycle.disconnect(DisconnectReason::Error).is_err());
    // The rejected transition must not mutate the recorded state or reason.
    assert_eq!(
        lifecycle.state(),
        &ConnectionState::Disconnected {
            reason: DisconnectReason::Normal
        }
    );
    Ok(())
}

#[test]
fn reconnect_consumes_one_fresh_event_and_rejects_rearm() -> Result<(), SdkError> {
    let mut lifecycle = ConnectionLifecycle::default();
    lifecycle.connected()?;

    assert_eq!(
        lifecycle.reconnect(ReconnectEvent::EstablishedConnectionFate)?,
        ReconnectAttempt {
            attempt: 0,
            event: ReconnectEvent::EstablishedConnectionFate,
        }
    );
    assert!(
        lifecycle
            .reconnect(ReconnectEvent::ExplicitCallerAction)
            .is_err(),
        "an in-flight attempt cannot be re-armed"
    );
    assert_eq!(
        lifecycle.state(),
        &ConnectionState::Reconnecting { attempt: 0 }
    );
    Ok(())
}

#[test]
fn failed_attempt_parks_until_another_fresh_event() -> Result<(), SdkError> {
    let mut lifecycle = ConnectionLifecycle::default();
    lifecycle.connected()?;
    lifecycle.reconnect(ReconnectEvent::EstablishedConnectionFate)?;
    lifecycle.reconnect_failed(DisconnectReason::Error)?;

    assert_eq!(
        lifecycle.state(),
        &ConnectionState::Disconnected {
            reason: DisconnectReason::Error,
        }
    );
    assert_eq!(
        lifecycle.reconnect(ReconnectEvent::ProvedOnlineTransition)?,
        ReconnectAttempt {
            attempt: 1,
            event: ReconnectEvent::ProvedOnlineTransition,
        }
    );
    Ok(())
}

#[test]
fn successful_connection_resets_reconnect_attempts() -> Result<(), SdkError> {
    let mut lifecycle = ConnectionLifecycle::default();
    lifecycle.connected()?;
    lifecycle.reconnect(ReconnectEvent::EstablishedConnectionFate)?;
    lifecycle.reconnect_failed(DisconnectReason::Error)?;
    assert_eq!(
        lifecycle.reconnect(ReconnectEvent::ExplicitCallerAction)?,
        ReconnectAttempt {
            attempt: 1,
            event: ReconnectEvent::ExplicitCallerAction,
        }
    );

    lifecycle.connected()?;
    assert_eq!(
        lifecycle.reconnect(ReconnectEvent::EstablishedConnectionFate)?,
        ReconnectAttempt {
            attempt: 0,
            event: ReconnectEvent::EstablishedConnectionFate,
        }
    );
    Ok(())
}
