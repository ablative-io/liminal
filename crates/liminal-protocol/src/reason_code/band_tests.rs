//! Pins the roster reason codes' wire values and their place in the band map.

use super::{CHANNEL_NOT_REGISTERED_CODE, CHANNEL_QUIESCED_CODE};

/// Ceiling of the protocol layer's band, the neighbour immediately below.
const PROTOCOL_BAND_CEILING: u16 = 0x00FF;
/// First value of the server layer's reserved band.
const SERVER_BAND_FIRST: u16 = 0x0100;
/// Last value of the server layer's reserved band.
const SERVER_BAND_LAST: u16 = 0x01FF;
/// The server's undifferentiated error, the far neighbour.
const UNDIFFERENTIATED_SERVER_ERROR: u16 = 0xFFFF;

#[test]
fn roster_reason_codes_hold_their_wire_values() {
    assert_eq!(CHANNEL_NOT_REGISTERED_CODE, 0x0101);
    assert_eq!(CHANNEL_QUIESCED_CODE, 0x0102);
    assert_ne!(CHANNEL_NOT_REGISTERED_CODE, CHANNEL_QUIESCED_CODE);
}

#[test]
fn roster_reason_codes_lie_inside_the_reserved_server_band() {
    for code in [CHANNEL_NOT_REGISTERED_CODE, CHANNEL_QUIESCED_CODE] {
        assert!((SERVER_BAND_FIRST..=SERVER_BAND_LAST).contains(&code));
    }
}

#[test]
fn roster_reason_codes_collide_with_neither_neighbouring_band() {
    for code in [CHANNEL_NOT_REGISTERED_CODE, CHANNEL_QUIESCED_CODE] {
        assert!(code > PROTOCOL_BAND_CEILING);
        assert_ne!(code, UNDIFFERENTIATED_SERVER_ERROR);
    }
}
