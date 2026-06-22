use alloc::rc::Rc;
use core::cell::RefCell;
use core::time::Duration;

use crate::SdkError;

use super::{
    ConnectionEvent, ConnectionLifecycle, ConnectionState, DisconnectReason, ReconnectConfig,
    ReconnectJitter,
};

#[derive(Debug)]
struct FixedJitter(Duration);

impl FixedJitter {
    const fn new(delay: Duration) -> Self {
        Self(delay)
    }
}

impl ReconnectJitter for FixedJitter {
    fn jitter(&mut self, attempt: u32, capped_delay: Duration) -> Duration {
        let _ = (attempt, capped_delay);
        self.0
    }
}

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
    let events = Rc::new(RefCell::new(Vec::new()));
    let observed = Rc::clone(&events);
    let mut lifecycle = ConnectionLifecycle::default();

    lifecycle.observe(move |event| observed.borrow_mut().push(event.clone()));
    lifecycle.connected()?;
    lifecycle.disconnect(DisconnectReason::Timeout)?;

    assert_eq!(events.borrow().len(), 2);
    let first = events.borrow()[0].clone();
    assert_eq!(
        first,
        ConnectionEvent::new(ConnectionState::Connecting, ConnectionState::Connected)
    );
    Ok(())
}

#[test]
fn disconnect_from_connecting_is_observable() -> Result<(), SdkError> {
    let events = Rc::new(RefCell::new(Vec::new()));
    let observed = Rc::clone(&events);
    let mut lifecycle = ConnectionLifecycle::default();

    // A fresh lifecycle starts in `Connecting`.
    assert_eq!(lifecycle.state(), &ConnectionState::Connecting);

    lifecycle.observe(move |event| observed.borrow_mut().push(event.clone()));
    lifecycle.disconnect(DisconnectReason::Error)?;

    assert_eq!(
        lifecycle.state(),
        &ConnectionState::Disconnected {
            reason: DisconnectReason::Error
        }
    );
    assert_eq!(events.borrow().len(), 1);
    assert_eq!(
        events.borrow()[0],
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
    let events = Rc::new(RefCell::new(Vec::new()));
    let observed = Rc::clone(&events);
    let mut lifecycle = ConnectionLifecycle::default();
    let mut jitter = FixedJitter::new(Duration::ZERO);

    lifecycle.connected()?;
    lifecycle.reconnect(&mut jitter)?;
    assert_eq!(
        lifecycle.state(),
        &ConnectionState::Reconnecting { attempt: 0 }
    );

    lifecycle.observe(move |event| observed.borrow_mut().push(event.clone()));
    lifecycle.disconnect(DisconnectReason::Error)?;

    assert_eq!(
        lifecycle.state(),
        &ConnectionState::Disconnected {
            reason: DisconnectReason::Error
        }
    );
    assert_eq!(events.borrow().len(), 1);
    assert_eq!(
        events.borrow()[0],
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
fn reconnect_delay_uses_exponential_backoff_and_jitter() -> Result<(), SdkError> {
    let config = ReconnectConfig::new(Duration::from_secs(1), Duration::from_secs(10));

    for attempt in 0_u32..=10 {
        // Independently assert the exponential formula `min(base * 2^attempt,
        // max_delay)` against hardcoded expectations so a wrong formula (e.g.
        // `3^attempt`) would fail here rather than being echoed back.
        let expected_capped = match attempt {
            0 => Duration::from_secs(1),
            1 => Duration::from_secs(2),
            2 => Duration::from_secs(4),
            3 => Duration::from_secs(8),
            _ => Duration::from_secs(10), // base * 2^attempt saturates at max_delay
        };
        let capped_delay = config.capped_delay(attempt);
        assert_eq!(capped_delay, expected_capped, "attempt {attempt}");

        let jitter = capped_delay / 2;
        let delay = config.retry_delay_with_jitter(attempt, jitter)?;

        assert_eq!(delay, expected_capped + jitter, "attempt {attempt}");
    }

    assert_eq!(
        config.retry_delay_with_jitter(0, Duration::from_millis(500))?,
        Duration::from_millis(1_500)
    );
    assert_eq!(
        config.retry_delay_with_jitter(3, Duration::from_secs(4))?,
        Duration::from_secs(12)
    );
    assert!(
        config
            .retry_delay_with_jitter(3, Duration::from_millis(4_001))
            .is_err()
    );
    Ok(())
}

#[test]
fn reconnect_delay_saturates_to_max_delay_on_large_attempts() {
    let config = ReconnectConfig::new(Duration::MAX, Duration::from_secs(30));

    assert_eq!(config.capped_delay(1), Duration::from_secs(30));
}

#[test]
fn successful_connection_resets_reconnect_attempts() -> Result<(), SdkError> {
    let mut lifecycle = ConnectionLifecycle::default();
    let mut jitter = FixedJitter::new(Duration::ZERO);

    lifecycle.connected()?;
    assert_eq!(
        lifecycle.reconnect(&mut jitter)?,
        Duration::from_millis(100)
    );
    assert_eq!(
        lifecycle.state(),
        &ConnectionState::Reconnecting { attempt: 0 }
    );
    assert_eq!(
        lifecycle.reconnect(&mut jitter)?,
        Duration::from_millis(200)
    );
    assert_eq!(
        lifecycle.state(),
        &ConnectionState::Reconnecting { attempt: 1 }
    );

    lifecycle.connected()?;
    assert_eq!(
        lifecycle.reconnect(&mut jitter)?,
        Duration::from_millis(100)
    );
    assert_eq!(
        lifecycle.state(),
        &ConnectionState::Reconnecting { attempt: 0 }
    );
    Ok(())
}
