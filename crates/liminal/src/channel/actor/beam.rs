//! The channel actor's beamr bytecode body, command NIF, and link plumbing.
//!
//! Mirrors `conversation/actor/beam.rs`. The actor runs a tiny bytecode loop
//! (`LoopRec` -> NIF -> tail-call) so it is a real `trap_exit` process: its NIF
//! receives every mailbox message, which is either the command-wakeup atom
//! (drain one queued command) or a trapped `{EXIT, pid, reason}` signal from a
//! dead subscriber (prune it from the fan-out list). Linking to *existing*
//! subscriber pids requires the `ProcessContext::link_facility()` available only
//! on this bytecode-NIF path — which is exactly why the channel actor is a
//! bytecode process and not a `NativeHandler`.

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};

use beamr::atom::Atom;
use beamr::constant_pool::ConstantPool;
use beamr::loader::Instruction;
use beamr::loader::decode::Operand;
use beamr::module::{Module, ModuleOrigin, ResolvedImport, ResolvedImportTarget};
use beamr::native::{Capability, NativeEntry, ProcessContext};
use beamr::term::Term;
use beamr::term::boxed::Tuple;

use super::ChannelActorCore;
use crate::error::LiminalError;

/// Per-scheduler registry mapping a channel actor's pid to its shared core, plus
/// the command-wakeup atom the NIF recognises. Stored as the scheduler's
/// `nif_private_data` so the NIF can recover the right core for the running pid.
pub struct ActorRuntime {
    command_atom: Atom,
    actors: Mutex<HashMap<u64, Weak<ChannelActorCore>>>,
}

impl std::fmt::Debug for ActorRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ActorRuntime")
            .field("command_atom", &self.command_atom)
            .finish_non_exhaustive()
    }
}

impl ActorRuntime {
    pub fn new(command_atom: Atom) -> Self {
        Self {
            command_atom,
            actors: Mutex::new(HashMap::new()),
        }
    }

    pub const fn command_atom(&self) -> Atom {
        self.command_atom
    }

    pub fn register(&self, pid: u64, core: Weak<ChannelActorCore>) -> Result<(), LiminalError> {
        self.actors
            .lock()
            .map_err(|error| LiminalError::DeliveryFailed {
                message: format!("channel actor runtime lock poisoned: {error}"),
            })?
            .insert(pid, core);
        Ok(())
    }

    fn actor(&self, pid: u64) -> Option<Arc<ChannelActorCore>> {
        self.actors
            .lock()
            .ok()
            .and_then(|actors| actors.get(&pid).and_then(Weak::upgrade))
    }
}

/// Build the channel actor's bytecode module: an endless `receive` loop that
/// hands each message to the `process_command` NIF. Identical in shape to the
/// conversation actor module.
pub fn actor_module(module_name: Atom, entry_function: Atom, command_function: Atom) -> Module {
    let code = vec![
        Instruction::Label { label: 1 },
        Instruction::LoopRec {
            fail: Operand::Label(2),
            destination: Operand::X(0),
        },
        Instruction::RemoveMessage,
        Instruction::CallExt {
            arity: Operand::Unsigned(1),
            import: Operand::Unsigned(0),
        },
        Instruction::CallOnly {
            arity: Operand::Unsigned(0),
            label: Operand::Label(1),
        },
        Instruction::Label { label: 2 },
        Instruction::Wait {
            fail: Operand::Label(1),
        },
    ];
    let label_index = code
        .iter()
        .enumerate()
        .filter_map(|(ip, instruction)| match instruction {
            Instruction::Label { label } => Some((*label, ip)),
            _ => None,
        })
        .collect();
    let mut exports = HashMap::new();
    exports.insert((entry_function, 0), 1);
    Module {
        name: module_name,
        generation: 0,
        origin: ModuleOrigin::Preloaded,
        exports,
        label_index,
        code,
        function_table: Vec::new(),
        line_table: Vec::new(),
        literals: Vec::new(),
        constant_pool: ConstantPool::default(),
        resolved_imports: vec![ResolvedImport {
            module: module_name,
            function: command_function,
            arity: 1,
            target: ResolvedImportTarget::Native(NativeEntry {
                function: process_command_nif,
                dirty_kind: None,
                capability: Capability::ProcessLocal,
            }),
        }],
        lambdas: Vec::new(),
        string_table: Vec::new(),
        line_info: Vec::new(),
    }
}

/// Establish a bidirectional beamr link from the channel actor to `subscriber`,
/// so the subscriber's exit is trapped as an `{EXIT, pid, reason}` message.
pub(super) fn link_subscriber(
    context: &ProcessContext<'_>,
    subscriber: u64,
) -> Result<(), LiminalError> {
    let actor_pid = context
        .pid()
        .ok_or_else(|| LiminalError::SubscriptionFailed {
            message: "channel actor has no beamr pid".to_owned(),
        })?;
    let link_facility =
        context
            .link_facility()
            .ok_or_else(|| LiminalError::SubscriptionFailed {
                message: "beamr link facility is unavailable".to_owned(),
            })?;
    link_facility
        .link(actor_pid, subscriber)
        .map_err(|error| LiminalError::SubscriptionFailed {
            message: format!(
                "failed to link channel actor {actor_pid} to subscriber {subscriber}: {error}"
            ),
        })
}

/// Remove the bidirectional link to `subscriber` (caller-driven unsubscribe).
pub(super) fn unlink_subscriber(
    context: &ProcessContext<'_>,
    subscriber: u64,
) -> Result<(), LiminalError> {
    let actor_pid = context
        .pid()
        .ok_or_else(|| LiminalError::SubscriptionFailed {
            message: "channel actor has no beamr pid".to_owned(),
        })?;
    let link_facility =
        context
            .link_facility()
            .ok_or_else(|| LiminalError::SubscriptionFailed {
                message: "beamr link facility is unavailable".to_owned(),
            })?;
    link_facility
        .unlink(actor_pid, subscriber)
        .map_err(|error| LiminalError::SubscriptionFailed {
            message: format!(
                "failed to unlink channel actor {actor_pid} from subscriber {subscriber}: {error}"
            ),
        })
}

fn process_command_nif(args: &[Term], context: &mut ProcessContext<'_>) -> Result<Term, Term> {
    let [message] = args else {
        return Err(badarg());
    };
    let pid = context.pid().ok_or_else(badarg)?;
    let runtime = runtime(context)?;
    let core = runtime.actor(pid).ok_or_else(badarg)?;
    if message.as_atom() == Some(runtime.command_atom) {
        if core.process_next_command(context) {
            context.request_shutdown();
        }
        return Ok(Term::atom(Atom::OK));
    }
    if let Some(subscriber) = exit_source(*message) {
        core.handle_subscriber_exit(subscriber)
            .map_err(|_| badarg())?;
    }
    Ok(Term::atom(Atom::OK))
}

fn runtime(context: &ProcessContext<'_>) -> Result<Arc<ActorRuntime>, Term> {
    let data = context.nif_private_data().ok_or_else(badarg)?;
    Arc::downcast::<ActorRuntime>(Arc::clone(data)).map_err(|_| badarg())
}

/// Decode a trapped `{EXIT, pid, reason}` tuple into the source pid.
pub fn exit_source(message: Term) -> Option<u64> {
    let tuple = Tuple::new(message)?;
    if tuple.arity() == 3 && tuple.get(0) == Some(Term::atom(Atom::EXIT)) {
        tuple.get(1)?.as_pid()
    } else {
        None
    }
}

const fn badarg() -> Term {
    Term::atom(Atom::BADARG)
}

/// Convenience for callers wiring a scheduler's private data.
pub fn private_data(runtime: Arc<ActorRuntime>) -> Arc<dyn Any + Send + Sync> {
    runtime
}
