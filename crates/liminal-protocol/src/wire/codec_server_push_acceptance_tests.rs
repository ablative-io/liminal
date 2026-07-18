use alloc::{vec, vec::Vec};

use crate::wire::{
    BindingEpoch, ConnectionIncarnation, DetachedCause, DiedCause, ParticipantDelivery,
    ParticipantRecord, ServerPush,
};

use super::{
    CodecError, DecodeClass, ParticipantFrame, ReceiverDirection, decode, encoded, generation,
};

pub(super) fn delivery(record: ParticipantRecord, sequence: u64) -> ParticipantFrame {
    ParticipantFrame::ServerPush(ServerPush::ParticipantDelivery(ParticipantDelivery {
        conversation_id: 41,
        delivery_seq: sequence,
        record,
    }))
}

fn push_frames() -> Result<Vec<ParticipantFrame>, CodecError> {
    let binding = BindingEpoch::new(ConnectionIncarnation::new(21, 22), generation(22)?);
    Ok(vec![
        ParticipantFrame::ServerPush(ServerPush::ObserverProgressed {
            conversation_id: 22,
            refused_epoch: 23,
            observer_progress: 24,
        }),
        delivery(
            ParticipantRecord::OrdinaryRecord {
                sender_participant_id: 25,
                payload: vec![1, 2, 3, 4],
            },
            26,
        ),
        delivery(
            ParticipantRecord::Attached {
                affected_participant_id: 27,
                binding_epoch: binding,
            },
            28,
        ),
        delivery(
            ParticipantRecord::Detached {
                affected_participant_id: 29,
                binding_epoch: binding,
                cause: DetachedCause::Superseded,
            },
            30,
        ),
        delivery(
            ParticipantRecord::Died {
                affected_participant_id: 31,
                binding_epoch: binding,
                cause: DiedCause::UncleanServerRestart {
                    prior_server_incarnation: 32,
                },
            },
            33,
        ),
        delivery(
            ParticipantRecord::Left {
                affected_participant_id: 34,
                ended_binding_epoch: Some(binding),
            },
            35,
        ),
        delivery(
            ParticipantRecord::HistoryCompacted {
                affected_participant_id: 36,
                abandoned_after: 37,
                abandoned_through: 38,
                physical_floor_at_decision: 39,
            },
            40,
        ),
    ])
}

fn exact_push_bytes(discriminant: u16, body: &[u8]) -> Result<Vec<u8>, CodecError> {
    let payload_len =
        u32::try_from(6_usize + body.len()).map_err(|_| CodecError::LengthOverflow)?;
    let mut bytes = vec![0x1A, 0, 0, 0, 0, 0];
    bytes.extend_from_slice(&payload_len.to_be_bytes());
    bytes.extend_from_slice(&1_u16.to_be_bytes());
    bytes.extend_from_slice(&0_u16.to_be_bytes());
    bytes.extend_from_slice(&discriminant.to_be_bytes());
    bytes.extend_from_slice(body);
    Ok(bytes)
}

#[test]
fn all_push_record_kinds_round_trip() -> Result<(), CodecError> {
    for frame in push_frames()? {
        let bytes = encoded(&frame)?;
        assert_eq!(decode(&bytes, ReceiverDirection::Client)?, frame);
    }
    Ok(())
}

#[test]
fn server_push_direction_and_codec_round_trip_all_record_kinds() -> Result<(), CodecError> {
    let frames = push_frames()?;
    let mut observer_body = Vec::new();
    observer_body.extend_from_slice(&22_u64.to_be_bytes());
    observer_body.extend_from_slice(&23_u64.to_be_bytes());
    observer_body.extend_from_slice(&24_u64.to_be_bytes());

    let mut ordinary_body = Vec::new();
    ordinary_body.extend_from_slice(&41_u64.to_be_bytes());
    ordinary_body.extend_from_slice(&26_u64.to_be_bytes());
    ordinary_body.extend_from_slice(&0_u16.to_be_bytes());
    ordinary_body.extend_from_slice(&25_u64.to_be_bytes());
    ordinary_body.extend_from_slice(&4_u32.to_be_bytes());
    ordinary_body.extend_from_slice(&[1, 2, 3, 4]);

    let binding_bytes = [
        21_u64.to_be_bytes(),
        22_u64.to_be_bytes(),
        22_u64.to_be_bytes(),
    ]
    .concat();
    let mut attached_body = Vec::new();
    attached_body.extend_from_slice(&41_u64.to_be_bytes());
    attached_body.extend_from_slice(&28_u64.to_be_bytes());
    attached_body.extend_from_slice(&1_u16.to_be_bytes());
    attached_body.extend_from_slice(&27_u64.to_be_bytes());
    attached_body.extend_from_slice(&binding_bytes);

    let mut detached_body = Vec::new();
    detached_body.extend_from_slice(&41_u64.to_be_bytes());
    detached_body.extend_from_slice(&30_u64.to_be_bytes());
    detached_body.extend_from_slice(&2_u16.to_be_bytes());
    detached_body.extend_from_slice(&29_u64.to_be_bytes());
    detached_body.extend_from_slice(&binding_bytes);
    detached_body.extend_from_slice(&5_u16.to_be_bytes());

    let mut died_body = Vec::new();
    died_body.extend_from_slice(&41_u64.to_be_bytes());
    died_body.extend_from_slice(&33_u64.to_be_bytes());
    died_body.extend_from_slice(&3_u16.to_be_bytes());
    died_body.extend_from_slice(&31_u64.to_be_bytes());
    died_body.extend_from_slice(&binding_bytes);
    died_body.extend_from_slice(&7_u16.to_be_bytes());
    died_body.extend_from_slice(&32_u64.to_be_bytes());

    let mut left_body = Vec::new();
    left_body.extend_from_slice(&41_u64.to_be_bytes());
    left_body.extend_from_slice(&35_u64.to_be_bytes());
    left_body.extend_from_slice(&4_u16.to_be_bytes());
    left_body.extend_from_slice(&34_u64.to_be_bytes());
    left_body.push(1);
    left_body.extend_from_slice(&binding_bytes);

    let mut compacted_body = Vec::new();
    compacted_body.extend_from_slice(&41_u64.to_be_bytes());
    compacted_body.extend_from_slice(&40_u64.to_be_bytes());
    compacted_body.extend_from_slice(&5_u16.to_be_bytes());
    compacted_body.extend_from_slice(&36_u64.to_be_bytes());
    compacted_body.extend_from_slice(&37_u64.to_be_bytes());
    compacted_body.extend_from_slice(&38_u64.to_be_bytes());
    compacted_body.extend_from_slice(&39_u64.to_be_bytes());

    let expected = [
        exact_push_bytes(0x0200, &observer_body)?,
        exact_push_bytes(0x0201, &ordinary_body)?,
        exact_push_bytes(0x0201, &attached_body)?,
        exact_push_bytes(0x0201, &detached_body)?,
        exact_push_bytes(0x0201, &died_body)?,
        exact_push_bytes(0x0201, &left_body)?,
        exact_push_bytes(0x0201, &compacted_body)?,
    ];
    for (frame, exact_bytes) in frames.iter().zip(expected) {
        assert_eq!(encoded(frame)?, exact_bytes);
        assert_eq!(decode(&exact_bytes, ReceiverDirection::Client)?, *frame);
        assert_eq!(
            decode(&exact_bytes, ReceiverDirection::Server),
            Err(CodecError::Decode {
                class: DecodeClass::UnknownDiscriminant,
            })
        );
    }
    Ok(())
}
