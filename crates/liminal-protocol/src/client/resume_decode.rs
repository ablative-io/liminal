use super::{
    ClientBindingState, DetachReplayStatus, DetachReplayTerminal, ExpectedOperationState,
    ReconnectFreshEvent,
    reconnect::ReconnectMachineState,
    replay::DetachReplayState,
    resume::{
        ClientResumeRecordDecodeError, ClientResumeRecordSection, DecodedFacts, MAGIC, VERSION,
    },
};
use crate::wire::{
    BindingEpoch, ConnectionIncarnation, DetachStaleAuthority, Generation, ParticipantFrame,
    ReceiverDirection, ServerValue, StaleAuthority, decode,
};

pub(super) fn decode_facts(input: &[u8]) -> Result<DecodedFacts, ClientResumeRecordDecodeError> {
    let mut reader = Reader::new(input);
    let presented = reader.array::<4>()?;
    if presented != MAGIC {
        return Err(ClientResumeRecordDecodeError::InvalidMagic { presented });
    }
    let version = reader.u16()?;
    if version != VERSION {
        return Err(ClientResumeRecordDecodeError::UnsupportedVersion { presented: version });
    }
    let declared = reader.u64()?;
    if usize::try_from(declared).ok() != Some(reader.remaining()) {
        return Err(ClientResumeRecordDecodeError::LengthMismatch {
            declared,
            actual: reader.remaining(),
        });
    }
    let binding = decode_binding(&mut reader)?;
    let next_operation_authorization = reader.u64()?;
    let expected = decode_expected(&mut reader)?;
    let replay = decode_replay(&mut reader)?;
    let (reconnect_state, next_authorization) = decode_reconnect(&mut reader)?;
    if reader.remaining() != 0 {
        return Err(ClientResumeRecordDecodeError::TrailingBytes {
            remaining: reader.remaining(),
        });
    }
    Ok(DecodedFacts {
        binding,
        next_operation_authorization,
        expected,
        replay,
        reconnect_state,
        next_authorization,
    })
}

fn decode_binding(
    reader: &mut Reader<'_>,
) -> Result<ClientBindingState, ClientResumeRecordDecodeError> {
    match reader.u8()? {
        0 => Ok(ClientBindingState::Unbound),
        1 => {
            let conversation_id = reader.u64()?;
            let participant_id = reader.u64()?;
            let generation = decode_generation(reader, ClientResumeRecordSection::Binding)?;
            let attach_secret = crate::wire::AttachSecret::new(reader.array::<32>()?);
            let binding_epoch = decode_epoch(reader)?;
            Ok(ClientBindingState::Bound {
                conversation_id,
                participant_id,
                generation,
                attach_secret,
                binding_epoch,
            })
        }
        tag @ (2 | 3) => {
            let conversation_id = reader.u64()?;
            let participant_id = reader.u64()?;
            let generation = decode_generation(reader, ClientResumeRecordSection::Binding)?;
            if tag == 2 {
                let attach_secret = crate::wire::AttachSecret::new(reader.array::<32>()?);
                Ok(ClientBindingState::Detached {
                    conversation_id,
                    participant_id,
                    generation,
                    attach_secret,
                })
            } else {
                Ok(ClientBindingState::Left {
                    conversation_id,
                    participant_id,
                    generation,
                })
            }
        }
        tag => Err(ClientResumeRecordDecodeError::InvalidTag {
            section: ClientResumeRecordSection::Binding,
            tag,
        }),
    }
}

fn decode_expected(
    reader: &mut Reader<'_>,
) -> Result<Option<ExpectedOperationState>, ClientResumeRecordDecodeError> {
    match reader.u8()? {
        0 => Ok(None),
        1 => {
            let issued = decode_bool(reader, ClientResumeRecordSection::ExpectedOperation)?;
            let authorization = reader.u64()?;
            let bytes = reader.blob()?;
            match decode(bytes, ReceiverDirection::Server) {
                Ok(ParticipantFrame::ClientRequest(request)) => Ok(Some(ExpectedOperationState {
                    request,
                    issued,
                    authorization,
                })),
                Ok(ParticipantFrame::ServerValue(_) | ParticipantFrame::ServerPush(_)) => {
                    Err(ClientResumeRecordDecodeError::NestedCodec {
                        section: ClientResumeRecordSection::ExpectedOperation,
                        source: None,
                    })
                }
                Err(source) => Err(ClientResumeRecordDecodeError::NestedCodec {
                    section: ClientResumeRecordSection::ExpectedOperation,
                    source: Some(source),
                }),
            }
        }
        tag => Err(ClientResumeRecordDecodeError::InvalidTag {
            section: ClientResumeRecordSection::ExpectedOperation,
            tag,
        }),
    }
}

fn decode_replay(
    reader: &mut Reader<'_>,
) -> Result<DetachReplayState, ClientResumeRecordDecodeError> {
    let tag = reader.u8()?;
    if tag == 0 {
        return Ok(DetachReplayState::Empty);
    }
    if !(1..=7).contains(&tag) {
        return Err(ClientResumeRecordDecodeError::InvalidTag {
            section: ClientResumeRecordSection::DetachReplay,
            tag,
        });
    }
    let request = crate::wire::DetachEnvelope {
        conversation_id: reader.u64()?,
        participant_id: reader.u64()?,
        capability_generation: decode_generation(reader, ClientResumeRecordSection::DetachReplay)?,
        detach_attempt_token: crate::wire::DetachAttemptToken::new(reader.array::<16>()?),
    };
    let status = match tag {
        1 => DetachReplayStatus::Parked,
        2 => DetachReplayStatus::InFlight,
        3 => DetachReplayStatus::Superseded,
        4 => DetachReplayStatus::LeaveSuperseded,
        5..=7 => decode_terminal(reader, tag)?,
        _ => {
            return Err(ClientResumeRecordDecodeError::InvalidTag {
                section: ClientResumeRecordSection::DetachReplay,
                tag,
            });
        }
    };
    Ok(DetachReplayState::Recorded { request, status })
}

fn decode_terminal(
    reader: &mut Reader<'_>,
    tag: u8,
) -> Result<DetachReplayStatus, ClientResumeRecordDecodeError> {
    let bytes = reader.blob()?;
    let value = match decode(bytes, ReceiverDirection::Client) {
        Ok(ParticipantFrame::ServerValue(value)) => value,
        Ok(ParticipantFrame::ClientRequest(_) | ParticipantFrame::ServerPush(_)) => {
            return Err(ClientResumeRecordDecodeError::NestedCodec {
                section: ClientResumeRecordSection::DetachReplay,
                source: None,
            });
        }
        Err(source) => {
            return Err(ClientResumeRecordDecodeError::NestedCodec {
                section: ClientResumeRecordSection::DetachReplay,
                source: Some(source),
            });
        }
    };
    let terminal = match (tag, value) {
        (5, ServerValue::DetachCommitted(value)) => DetachReplayTerminal::DetachCommitted(value),
        (6, ServerValue::DetachInProgress(value)) => DetachReplayTerminal::DetachInProgress(value),
        (
            7,
            ServerValue::StaleAuthority(StaleAuthority::Detach(
                DetachStaleAuthority::TerminalizedDetachCell(value),
            )),
        ) => DetachReplayTerminal::TerminalizedDetachCell(value),
        _ => {
            return Err(ClientResumeRecordDecodeError::NestedCodec {
                section: ClientResumeRecordSection::DetachReplay,
                source: None,
            });
        }
    };
    Ok(DetachReplayStatus::Terminal(terminal))
}

fn decode_reconnect(
    reader: &mut Reader<'_>,
) -> Result<(ReconnectMachineState, u64), ClientResumeRecordDecodeError> {
    let next_authorization = reader.u64()?;
    let state = match reader.u8()? {
        0 => ReconnectMachineState::Parked,
        1 => {
            let authorization = reader.u64()?;
            let event = decode_event(reader)?;
            let issued = decode_bool(reader, ClientResumeRecordSection::Reconnect)?;
            ReconnectMachineState::Permit {
                authorization,
                event,
                issued,
            }
        }
        2 => ReconnectMachineState::Attempt {
            authorization: reader.u64()?,
            event: decode_event(reader)?,
        },
        3 => ReconnectMachineState::Online,
        tag => {
            return Err(ClientResumeRecordDecodeError::InvalidTag {
                section: ClientResumeRecordSection::Reconnect,
                tag,
            });
        }
    };
    Ok((state, next_authorization))
}

fn decode_event(
    reader: &mut Reader<'_>,
) -> Result<ReconnectFreshEvent, ClientResumeRecordDecodeError> {
    match reader.u8()? {
        0 => Ok(ReconnectFreshEvent::TransportFate(
            super::EstablishedConnectionTransportFate::Lost,
        )),
        1 => Ok(ReconnectFreshEvent::OnlineTransition(
            super::ProvedOnlineTransition::ProvedOnline,
        )),
        2 => Ok(ReconnectFreshEvent::ExplicitCallerAction(
            super::ExplicitReconnectAction::ReconnectNow,
        )),
        tag => Err(ClientResumeRecordDecodeError::InvalidTag {
            section: ClientResumeRecordSection::Reconnect,
            tag,
        }),
    }
}

fn decode_bool(
    reader: &mut Reader<'_>,
    section: ClientResumeRecordSection,
) -> Result<bool, ClientResumeRecordDecodeError> {
    match reader.u8()? {
        0 => Ok(false),
        1 => Ok(true),
        tag => Err(ClientResumeRecordDecodeError::InvalidTag { section, tag }),
    }
}

fn decode_generation(
    reader: &mut Reader<'_>,
    section: ClientResumeRecordSection,
) -> Result<Generation, ClientResumeRecordDecodeError> {
    Generation::new(reader.u64()?).ok_or(ClientResumeRecordDecodeError::NestedCodec {
        section,
        source: None,
    })
}

fn decode_epoch(reader: &mut Reader<'_>) -> Result<BindingEpoch, ClientResumeRecordDecodeError> {
    Ok(BindingEpoch::new(
        ConnectionIncarnation::new(reader.u64()?, reader.u64()?),
        decode_generation(reader, ClientResumeRecordSection::Binding)?,
    ))
}

struct Reader<'a> {
    input: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    const fn new(input: &'a [u8]) -> Self {
        Self { input, offset: 0 }
    }

    const fn remaining(&self) -> usize {
        self.input.len().saturating_sub(self.offset)
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], ClientResumeRecordDecodeError> {
        let remaining = self.remaining();
        if remaining < length {
            return Err(ClientResumeRecordDecodeError::Truncated {
                needed: length,
                remaining,
            });
        }
        let start = self.offset;
        self.offset += length;
        Ok(&self.input[start..self.offset])
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], ClientResumeRecordDecodeError> {
        let mut output = [0; N];
        output.copy_from_slice(self.take(N)?);
        Ok(output)
    }

    fn u8(&mut self) -> Result<u8, ClientResumeRecordDecodeError> {
        Ok(self.array::<1>()?[0])
    }

    fn u16(&mut self) -> Result<u16, ClientResumeRecordDecodeError> {
        Ok(u16::from_be_bytes(self.array()?))
    }

    fn u64(&mut self) -> Result<u64, ClientResumeRecordDecodeError> {
        Ok(u64::from_be_bytes(self.array()?))
    }

    fn blob(&mut self) -> Result<&'a [u8], ClientResumeRecordDecodeError> {
        let length = self.u64()?;
        let length =
            usize::try_from(length).map_err(|_| ClientResumeRecordDecodeError::LengthMismatch {
                declared: length,
                actual: self.remaining(),
            })?;
        self.take(length)
    }
}
