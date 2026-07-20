use std::error::Error;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};

use liminal::durability::{DurableStore, bridge::block_on, open_ephemeral};
use liminal_protocol::wire::{ClientRequest, ServerValue};

use super::{ConnectionSupervisor, ParticipantSemanticHandler};
use crate::ServerError;
use crate::config::types::LimitsConfig;
use crate::server::connection::services::{ConnectionServices, LiminalConnectionServices};
use crate::server::participant::incarnation_stream::{ConnectionFateClass, IncarnationStream};
use crate::server::participant::{
    ConnectionFateWorkItem, InstalledParticipantService, ParticipantConnectionContext,
    ParticipantConnectionConversations, ParticipantSemanticError, ParticipantServiceFatal,
};
use crate::server::shutdown::ShutdownHandle;

const FIXTURE_CONVERSATION_LIMIT: u64 = 1;

#[derive(Debug, Default)]
struct FailingFateHandler {
    fatal: Mutex<Option<ParticipantServiceFatal>>,
}

impl ParticipantSemanticHandler for FailingFateHandler {
    fn service_fatal(&self) -> Result<Option<ParticipantServiceFatal>, ParticipantSemanticError> {
        self.fatal.lock().map(|fatal| fatal.clone()).map_err(|_| {
            ParticipantSemanticError::Internal {
                message: "fatal-composition fixture latch is poisoned".to_owned(),
            }
        })
    }

    fn latch_connection_fate_intent_incomplete(
        &self,
        open_sequence: u64,
        conversation_id: u64,
    ) -> Result<ParticipantServiceFatal, ParticipantSemanticError> {
        let fatal = self
            .fatal
            .lock()
            .map_err(|_| ParticipantSemanticError::Internal {
                message: "fatal-composition fixture latch is poisoned".to_owned(),
            })?
            .get_or_insert_with(|| ParticipantServiceFatal::ConnectionFateIntentIncomplete {
                open_sequence,
                conversation_id,
            })
            .clone();
        Ok(fatal)
    }

    fn handle_connection_fate(
        &self,
        work_item: ConnectionFateWorkItem,
    ) -> Result<(), ParticipantSemanticError> {
        Err(ParticipantSemanticError::Internal {
            message: format!(
                "injected failure while completing Open {}",
                work_item.open_sequence
            ),
        })
    }

    fn publication_conversation_limit(&self) -> u64 {
        FIXTURE_CONVERSATION_LIMIT
    }

    fn handle(
        &self,
        context: ParticipantConnectionContext,
        conversations: &mut ParticipantConnectionConversations,
        request: ClientRequest,
    ) -> Result<ServerValue, ParticipantSemanticError> {
        let _ = (context, conversations, request);
        Err(ParticipantSemanticError::Unavailable)
    }
}

fn tcp_pair() -> Result<(TcpStream, TcpStream), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address: SocketAddr = listener.local_addr()?;
    let client = TcpStream::connect(address)?;
    let (server, _) = listener.accept()?;
    Ok((client, server))
}

fn stream_len(store: &Arc<dyn DurableStore>) -> Result<usize, Box<dyn Error>> {
    Ok(block_on(store.read_from(
        IncarnationStream::stream_key(),
        0,
        LimitsConfig::default().max_connections,
    ))??
    .len())
}

#[test]
fn post_open_fatal_stops_new_opens_and_admission_and_activates_normal_shutdown()
-> Result<(), Box<dyn Error>> {
    const CONVERSATION_ID: u64 = 31;

    let store: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    let handler = Arc::new(FailingFateHandler::default());
    let participant_service =
        InstalledParticipantService::new(handler, Arc::clone(&store), u64::MAX)
            .map_err(|error| format!("participant service configuration failed: {error:?}"))?;
    let services: Arc<dyn ConnectionServices> =
        Arc::new(LiminalConnectionServices::empty()?.with_participant_service(participant_service));
    let shutdown = ShutdownHandle::new();
    let supervisor = ConnectionSupervisor::with_fatal_shutdown(
        services,
        None,
        LimitsConfig::default(),
        shutdown.clone(),
    )?;
    let (client, server) = tcp_pair()?;
    let connection = supervisor.spawn_connection(server)?;
    let incarnation = connection
        .connection_incarnation()
        .ok_or("participant connection lacks a durable incarnation")?;
    let entries_before_open = stream_len(&store)?;

    let failed = supervisor.inner.runtime.complete_connection_fate(
        Some(incarnation),
        ConnectionFateClass::ConnectionLost,
        &[CONVERSATION_ID],
    );
    let failure = match failed {
        Ok(()) => return Err("injected post-Open handler failure unexpectedly completed".into()),
        Err(error) => error,
    };
    let ServerError::ParticipantServiceFatal { fatal } = failure else {
        return Err(format!("post-Open failure returned the wrong server error: {failure}").into());
    };
    let ParticipantServiceFatal::ConnectionFateIntentIncomplete {
        open_sequence,
        conversation_id,
    } = &fatal;
    assert_eq!(*conversation_id, CONVERSATION_ID);
    assert!(shutdown.is_initiated());
    let observed_fatal = supervisor.participant_service_fatal()?;
    assert_eq!(observed_fatal.as_ref(), Some(&fatal));

    let entries_after_open = stream_len(&store)?;
    assert_eq!(
        entries_after_open,
        entries_before_open
            .checked_add(1)
            .ok_or("incarnation-stream fixture length overflow")?,
        "the failed fold must leave exactly its durable Open unmatched"
    );

    let stopped_open = supervisor.inner.runtime.complete_connection_fate(
        Some(incarnation),
        ConnectionFateClass::ServerShutdown,
        &[CONVERSATION_ID],
    );
    assert!(matches!(
        stopped_open,
        Err(ServerError::ParticipantServiceFatal { fatal: observed }) if observed == fatal
    ));
    assert_eq!(
        stream_len(&store)?,
        entries_after_open,
        "a latched service must not append another Open"
    );

    let (refused_client, refused_server) = tcp_pair()?;
    let refused = supervisor.spawn_connection(refused_server);
    assert!(matches!(
        refused,
        Err(ServerError::ParticipantServiceFatal { fatal: observed }) if observed == fatal
    ));
    assert_eq!(supervisor.active_connection_count(), 1);
    assert_eq!(stream_len(&store)?, entries_after_open);
    assert!(
        *open_sequence > 0,
        "the fatal must name the durable Open sequence"
    );

    drop(refused_client);
    drop(client);
    supervisor.shutdown();
    Ok(())
}
