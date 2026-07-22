use std::error::Error;

use super::SocketFixture;

const KEEPALIVE_INTERVAL_MS: u64 = 10;
const REQUIRED_KEEPALIVE_INTERVALS: usize = 2;

#[test]
fn obligation_debt_dispatch_idle_has_zero_debt_attributable_work() -> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let fixture = SocketFixture::start(&home.path().join("idle"))?;
    let disabled_work = fixture.obligation_dispatch_work_snapshot();
    let disabled_ready_fires = fixture.publication_ready_fire_count();

    let mut endpoint =
        fixture.spawn_websocket_peer_with_ping_interval(Some(KEEPALIVE_INTERVAL_MS))?;
    let websocket_pid = endpoint.peer.pid();
    let idle_work_before = fixture.obligation_dispatch_work_snapshot();
    let ready_fires_before = fixture.publication_ready_fire_count();
    let slices_before = fixture.slice_count(websocket_pid);
    let mut ping_count = 0_usize;
    let ping_count_before = ping_count;

    for _interval in 0..REQUIRED_KEEPALIVE_INTERVALS {
        endpoint.peer.read_keepalive_ping()?;
        ping_count = ping_count
            .checked_add(1)
            .ok_or("transport Ping observation count overflowed")?;
    }

    let slices_after = fixture.slice_count(websocket_pid);
    let ping_count_after = ping_count;
    let idle_work_after = fixture.obligation_dispatch_work_snapshot();
    let ready_fires_after = fixture.publication_ready_fire_count();

    assert_eq!(
        idle_work_before, disabled_work,
        "adding an empty configured WebSocket must not perform W2 work"
    );
    assert_eq!(ready_fires_before, disabled_ready_fires);
    assert_eq!(
        idle_work_after, idle_work_before,
        "selector, authority-lock, outbox-probe, and W2 allocation counters must all stay flat"
    );
    assert_eq!(ready_fires_after, ready_fires_before);
    assert!(
        slices_after > slices_before,
        "the configured keepalive must grow the real scheduler slice counter"
    );
    assert!(
        ping_count_after
            .checked_sub(ping_count_before)
            .is_some_and(|observed| observed >= REQUIRED_KEEPALIVE_INTERVALS),
        "multiple real transport Ping observations must grow while W2 work stays flat"
    );

    endpoint.stop()?;
    fixture.stop();
    Ok(())
}
