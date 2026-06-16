use std::sync::mpsc;

use crate::conversation::types::ConversationState;
use crate::envelope::Envelope;
use crate::error::LiminalError;

pub(super) struct QueuedCommand {
    pub(super) id: u64,
    pub(super) kind: QueuedCommandKind,
}

pub(super) enum QueuedCommandKind {
    Boot {
        reply: mpsc::SyncSender<Result<(), LiminalError>>,
    },
    Send {
        message: Envelope,
        reply: mpsc::SyncSender<Result<(), LiminalError>>,
    },
    Receive {
        reply: mpsc::SyncSender<Result<Envelope, LiminalError>>,
    },
    Close {
        reply: mpsc::SyncSender<Result<(), LiminalError>>,
    },
    QueryState {
        reply: mpsc::SyncSender<Result<ConversationState, LiminalError>>,
    },
}
