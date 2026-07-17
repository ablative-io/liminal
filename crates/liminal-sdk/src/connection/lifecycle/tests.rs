use alloc::sync::Arc;

use spin::Mutex;

use crate::SdkError;

use super::{ConnectionEvent, ConnectionLifecycle, ConnectionState, DisconnectReason};

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
    assert_eq!(
        events.lock()[0],
        ConnectionEvent::new(ConnectionState::Connecting, ConnectionState::Connected)
    );
    Ok(())
}

#[test]
fn disconnect_from_connecting_is_observable() -> Result<(), SdkError> {
    let events = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&events);
    let mut lifecycle = ConnectionLifecycle::default();

    lifecycle.observe(move |event| observed.lock().push(event.clone()));
    lifecycle.disconnect(DisconnectReason::Error)?;

    let disconnected = ConnectionState::Disconnected {
        reason: DisconnectReason::Error,
    };
    assert_eq!(lifecycle.state(), &disconnected);
    assert_eq!(
        events.lock()[0],
        ConnectionEvent::new(ConnectionState::Connecting, disconnected)
    );
    Ok(())
}

#[test]
fn externally_authorized_reconnect_has_no_delay_or_retry_counter() -> Result<(), SdkError> {
    let mut lifecycle = ConnectionLifecycle::default();
    lifecycle.connected()?;

    lifecycle.reconnect_started()?;
    assert_eq!(lifecycle.state(), &ConnectionState::Reconnecting);
    assert!(lifecycle.reconnect_started().is_err());

    lifecycle.connected()?;
    lifecycle.reconnect_started()?;
    assert_eq!(lifecycle.state(), &ConnectionState::Reconnecting);
    Ok(())
}

#[test]
fn disconnect_from_reconnecting_is_observable() -> Result<(), SdkError> {
    let events = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&events);
    let mut lifecycle = ConnectionLifecycle::default();
    lifecycle.connected()?;
    lifecycle.reconnect_started()?;

    lifecycle.observe(move |event| observed.lock().push(event.clone()));
    lifecycle.disconnect(DisconnectReason::Error)?;

    assert_eq!(
        events.lock()[0],
        ConnectionEvent::new(
            ConnectionState::Reconnecting,
            ConnectionState::Disconnected {
                reason: DisconnectReason::Error,
            },
        )
    );
    Ok(())
}

#[test]
fn disconnect_from_disconnected_is_rejected_without_mutation() -> Result<(), SdkError> {
    let mut lifecycle = ConnectionLifecycle::default();
    lifecycle.disconnect(DisconnectReason::Normal)?;
    assert!(lifecycle.disconnect(DisconnectReason::Error).is_err());
    assert_eq!(
        lifecycle.state(),
        &ConnectionState::Disconnected {
            reason: DisconnectReason::Normal,
        }
    );
    Ok(())
}
