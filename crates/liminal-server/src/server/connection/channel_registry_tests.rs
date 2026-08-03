use super::{ChannelAccessError, ChannelConfigField, ChannelRegistryError};
use liminal_protocol::reason_code::{CHANNEL_NOT_REGISTERED_CODE, CHANNEL_QUIESCED_CODE};

/// The typed refusals carry the reserved server-band codes, and the internal
/// fault carries the undifferentiated one. Pinned against the protocol crate's
/// consts (not against literals) on two of the three, and against the literal
/// `0xFFFF` on the third — which is the value the band map records and this
/// module re-declares.
#[test]
fn access_refusals_carry_their_reserved_reason_codes() {
    assert_eq!(
        ChannelAccessError::NotRegistered {
            name: "orders".to_owned()
        }
        .reason_code(),
        CHANNEL_NOT_REGISTERED_CODE
    );
    assert_eq!(
        ChannelAccessError::Quiesced {
            name: "orders".to_owned(),
            reason: "archived".to_owned()
        }
        .reason_code(),
        CHANNEL_QUIESCED_CODE
    );
    assert_eq!(
        ChannelAccessError::RosterUnavailable {
            message: "poisoned".to_owned()
        }
        .reason_code(),
        0xFFFF
    );
}

/// The two roster codes are DIFFERENT from each other and from the
/// undifferentiated one: a client that branches on the code must be able to tell
/// "no such channel" from "quiesced" from "something else went wrong".
#[test]
fn the_three_access_codes_are_mutually_distinct() {
    let absent = ChannelAccessError::NotRegistered {
        name: "orders".to_owned(),
    }
    .reason_code();
    let quiesced = ChannelAccessError::Quiesced {
        name: "orders".to_owned(),
        reason: "archived".to_owned(),
    }
    .reason_code();
    let fault = ChannelAccessError::RosterUnavailable {
        message: "poisoned".to_owned(),
    }
    .reason_code();
    assert_ne!(absent, quiesced);
    assert_ne!(absent, fault);
    assert_ne!(quiesced, fault);
}

/// The mismatch field renders as prose inside the refusal, so an operator reads
/// which field differed while a program branches on the variant.
#[test]
fn already_registered_names_the_differing_field() {
    for (field, rendered) in [
        (ChannelConfigField::Mode, "mode"),
        (ChannelConfigField::SchemaId, "schema id"),
        (ChannelConfigField::SchemaDocument, "schema document"),
    ] {
        let error = ChannelRegistryError::AlreadyRegistered {
            name: "orders".to_owned(),
            field,
        };
        assert_eq!(
            error.to_string(),
            format!("channel 'orders' is already registered with a different {rendered}")
        );
    }
}

/// The cap refusals name the config key, so an operator is told what to declare
/// rather than that something is missing.
#[test]
fn cap_refusals_name_the_config_key() {
    let absent = ChannelRegistryError::CapNotConfigured {
        cap: super::super::channel_registry::MAX_CHANNELS_KEY,
    };
    assert!(absent.to_string().contains("limits.max_channels"));
    let reached = ChannelRegistryError::CapReached {
        cap: super::super::channel_registry::MAX_CHANNELS_KEY,
        limit: 4,
    };
    assert_eq!(
        reached.to_string(),
        "channel registration refused: the limits.max_channels limit of 4 is reached"
    );
}
