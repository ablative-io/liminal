//! Conversation pattern helpers built on the LIM-004 conversation actor.
//!
//! LIM-005 scopes three patterns (request-reply, streaming, dispatch). This
//! module implements the natural first one as a REAL reference: request-reply
//! ([`ask`]). The caller spawns a conversation linking it to a real participant
//! process, sends one request, and receives one reply that the participant
//! genuinely produced — the conversation then closes automatically. Handler
//! crash surfaces as [`LiminalError::ParticipantCrashed`]; exceeding the bound
//! surfaces as [`LiminalError::ConversationTimeout`].

use std::sync::Arc;
use std::time::Duration;

use crate::channel::ChannelMode;
use crate::conversation::participant::ParticipantBehaviour;
use crate::conversation::types::CrashPolicy;
use crate::conversation::{ConversationSupervisor, ParticipantPid};
use crate::envelope::Envelope;
use crate::error::LiminalError;

/// Default bound for an `ask` exchange when none is supplied by the caller.
pub const DEFAULT_ASK_TIMEOUT: Duration = Duration::from_secs(5);

/// Performs a single request-reply exchange against a fresh participant running
/// `behaviour`.
///
/// Spawns a real participant native process and a conversation actor linked to
/// it, forwards `request` to the participant, waits up to `timeout` for the
/// participant's reply, then closes the conversation. The participant process is
/// genuine (a beamr `NativeHandler`), so the returned [`Envelope`] is the
/// participant's actual processing result — not a loopback of the request
/// through an inert stand-in.
///
/// # Errors
/// - [`LiminalError::ParticipantCrashed`] if the participant crashes before
///   replying (detected structurally via the linked-EXIT, not by timeout).
/// - [`LiminalError::ConversationTimeout`] if no reply arrives within `timeout`.
/// - [`LiminalError`] for spawn, forward, or close failures.
pub fn ask(
    supervisor: &ConversationSupervisor,
    behaviour: Arc<dyn ParticipantBehaviour>,
    request: Envelope,
    timeout: Duration,
) -> Result<Envelope, LiminalError> {
    let (actor, _participant): (_, ParticipantPid) = supervisor.spawn_with_participant(
        behaviour,
        Some(timeout),
        ChannelMode::Ephemeral,
        CrashPolicy::Fail,
    )?;

    // Send the request; the actor forwards it to the participant process, which
    // processes it and delivers the reply back into the conversation.
    actor.handle().send(request)?;

    // Wait for the participant's reply. A participant crash drains this receive
    // with ParticipantCrashed; exceeding the bound yields ConversationTimeout.
    let reply = actor.receive_timeout(timeout);

    // Request-reply closes automatically once the exchange completes. A failed
    // conversation (crash) cannot transition to Closed, so closing best-effort
    // there is correct and not an error to propagate over the original cause.
    match reply {
        Ok(reply) => {
            actor.handle().close()?;
            Ok(reply)
        }
        Err(error) => {
            let _ = actor.handle().close();
            Err(error)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use beamr::process::ExitReason;

    use super::{DEFAULT_ASK_TIMEOUT, ask};
    use crate::channel::{ChannelMode, SchemaId};
    use crate::conversation::participant::{EchoBehaviour, ParticipantBehaviour};
    use crate::conversation::types::CrashPolicy;
    use crate::conversation::{ConversationSupervisor, ParticipantPid};
    use crate::envelope::{Envelope, PublisherId};
    use crate::error::LiminalError;

    fn request(payload: &[u8]) -> Envelope {
        Envelope::new(
            payload.to_vec(),
            None,
            SchemaId::new(),
            PublisherId::default(),
        )
    }

    /// A behaviour that never replies, so `ask` must hit the timeout path.
    #[derive(Debug)]
    struct SilentBehaviour;

    impl ParticipantBehaviour for SilentBehaviour {
        fn process(&self, _request: &Envelope) -> Option<Envelope> {
            None
        }
    }

    /// THE REQUEST-REPLY PROOF: `ask` returns the participant's REAL reply.
    ///
    /// This fails against the inert `spawn_test_process` participant: that
    /// process processes nothing, so it would never produce a reply and `ask`
    /// would time out instead of returning the transformed payload. The payload
    /// here is uppercased by a real behaviour, proving the participant actually
    /// ran — a loopback of the request would return the lowercase bytes.
    #[test]
    fn ask_returns_real_participant_reply() -> Result<(), Box<dyn std::error::Error>> {
        #[derive(Debug)]
        struct UppercaseBehaviour;
        impl ParticipantBehaviour for UppercaseBehaviour {
            fn process(&self, request: &Envelope) -> Option<Envelope> {
                let mut reply = request.clone();
                reply.payload = request.payload.to_ascii_uppercase();
                Some(reply)
            }
        }

        let supervisor = ConversationSupervisor::new()?;
        let reply = ask(
            &supervisor,
            Arc::new(UppercaseBehaviour),
            request(b"ping"),
            DEFAULT_ASK_TIMEOUT,
        )?;
        assert_eq!(
            reply.payload, b"PING",
            "ask must return the participant's processed reply, not a loopback"
        );
        supervisor.shutdown();
        Ok(())
    }

    #[test]
    fn ask_echo_round_trips_payload() -> Result<(), Box<dyn std::error::Error>> {
        let supervisor = ConversationSupervisor::new()?;
        let reply = ask(
            &supervisor,
            Arc::new(EchoBehaviour),
            request(b"hello-world"),
            DEFAULT_ASK_TIMEOUT,
        )?;
        assert_eq!(reply.payload, b"hello-world");
        supervisor.shutdown();
        Ok(())
    }

    #[test]
    fn ask_times_out_when_participant_never_replies() -> Result<(), Box<dyn std::error::Error>> {
        let supervisor = ConversationSupervisor::new()?;
        let result = ask(
            &supervisor,
            Arc::new(SilentBehaviour),
            request(b"unanswered"),
            Duration::from_millis(150),
        );
        assert!(
            matches!(result, Err(LiminalError::ConversationTimeout { .. })),
            "a non-replying participant must yield ConversationTimeout, got {result:?}"
        );
        supervisor.shutdown();
        Ok(())
    }

    /// Handler crash BEFORE a reply must surface as `ParticipantCrashed`, not a
    /// timeout — the crash is detected structurally via the linked EXIT.
    #[test]
    fn ask_reports_participant_crash_before_reply() -> Result<(), Box<dyn std::error::Error>> {
        let supervisor = ConversationSupervisor::new()?;
        let (actor, participant): (_, ParticipantPid) = supervisor.spawn_with_participant(
            Arc::new(SilentBehaviour),
            Some(Duration::from_secs(5)),
            ChannelMode::Ephemeral,
            CrashPolicy::Fail,
        )?;

        // Kill the participant before any reply, then issue the receive. The
        // linked EXIT drains the pending receive with ParticipantCrashed.
        supervisor
            .scheduler()
            .terminate_process(participant.get(), ExitReason::Error);

        // Drive the actor command loop so it processes the trapped EXIT; the
        // bound only guards against a genuine hang.
        let mut result = Err(LiminalError::ConversationTimeout {
            message: "init".to_owned(),
        });
        for _ in 0..1_000 {
            result = actor.receive_timeout(Duration::from_millis(20));
            if matches!(result, Err(LiminalError::ParticipantCrashed { .. })) {
                break;
            }
            std::thread::yield_now();
        }
        assert!(
            matches!(result, Err(LiminalError::ParticipantCrashed { .. })),
            "participant crash before reply must surface as ParticipantCrashed, got {result:?}"
        );
        supervisor.shutdown();
        Ok(())
    }
}
