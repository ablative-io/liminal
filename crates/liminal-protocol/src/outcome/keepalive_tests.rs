use alloc::string::String;

use super::keepalive::{
    AcceptedSocketKeepaliveReason, KeepaliveCertificationFailed, KeepaliveField, KeepaliveOption,
    KeepalivePhase, KeepaliveValue, PlatformName, StartupKeepaliveReason,
};

fn platform() -> PlatformName {
    PlatformName::new(String::from("test-platform"))
}

#[test]
fn startup_phase_has_exactly_the_five_startup_reason_shapes() {
    let reasons = [
        StartupKeepaliveReason::Zero {
            field: KeepaliveField::IdleSeconds,
            requested: 0,
            required_minimum: 1,
        },
        StartupKeepaliveReason::OutOfRange {
            field: KeepaliveField::IntervalSeconds,
            requested: 100,
            supported_min: 1,
            supported_max: 60,
            platform: platform(),
        },
        StartupKeepaliveReason::GranularityMismatch {
            field: KeepaliveField::ProbeCount,
            requested: 3,
            granularity: 2,
            platform: platform(),
        },
        StartupKeepaliveReason::UnsupportedPlatform {
            platform: platform(),
        },
        StartupKeepaliveReason::UnsupportedOption {
            option: KeepaliveOption::SoKeepalive,
            platform: platform(),
        },
    ];

    assert_eq!(reasons.len(), 5);
    for reason in reasons {
        let outcome = KeepaliveCertificationFailed::StartupConfiguration(reason);
        assert_eq!(outcome.phase(), KeepalivePhase::StartupConfiguration);
    }
}

#[test]
fn accepted_socket_phase_has_exactly_the_three_runtime_reason_shapes() {
    let reasons = [
        AcceptedSocketKeepaliveReason::SetFailed {
            option: KeepaliveOption::Idle,
            platform: platform(),
            os_error: 22,
        },
        AcceptedSocketKeepaliveReason::ReadbackFailed {
            option: KeepaliveOption::Interval,
            platform: platform(),
            os_error: 5,
        },
        AcceptedSocketKeepaliveReason::ReadbackMismatch {
            option: KeepaliveOption::Count,
            requested: KeepaliveValue::Unsigned(3),
            effective: KeepaliveValue::Unsigned(2),
            platform: platform(),
        },
    ];

    assert_eq!(reasons.len(), 3);
    for reason in reasons {
        let outcome = KeepaliveCertificationFailed::AcceptedSocket(reason);
        assert_eq!(outcome.phase(), KeepalivePhase::AcceptedSocket);
    }
}

#[test]
fn platform_name_round_trips_without_loss() {
    let name = platform();
    assert_eq!(name.as_str(), "test-platform");
    assert_eq!(name.into_string(), String::from("test-platform"));
}
