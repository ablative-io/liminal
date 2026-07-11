use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};
use std::time::Instant;

use beamr::atom::Atom;
use beamr::constant_pool::ConstantPool;
use beamr::loader::Instruction;
use beamr::loader::decode::Operand;
use beamr::module::{Module, ModuleOrigin, ResolvedImport, ResolvedImportTarget};
use beamr::native::{Capability, NativeEntry, ProcessContext};
use beamr::process::ExitReason;
use beamr::term::Term;
use beamr::term::boxed::Tuple;

use super::ActorCore;
use super::sync::lock;
use crate::conversation::types::ParticipantPid;
use crate::error::LiminalError;

pub(super) struct ActorRuntime {
    command_atom: Atom,
    actors: Mutex<HashMap<u64, Weak<ActorCore>>>,
}

impl std::fmt::Debug for ActorRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let actor_count = self.actors.lock().map_or(0, |actors| actors.len());
        formatter
            .debug_struct("ActorRuntime")
            .field("command_atom", &self.command_atom)
            .field("actor_count", &actor_count)
            .finish()
    }
}

impl ActorRuntime {
    pub(super) fn new(command_atom: Atom) -> Self {
        Self {
            command_atom,
            actors: Mutex::new(HashMap::new()),
        }
    }

    pub(super) const fn command_atom(&self) -> Atom {
        self.command_atom
    }

    pub(super) fn register(
        &self,
        pid: ParticipantPid,
        core: Weak<ActorCore>,
    ) -> Result<(), LiminalError> {
        let mut actors = lock(&self.actors, "actor runtime")?;
        // Drop keys whose actor core is already gone before inserting, so an actor
        // that exited without a close/restart deregistering it (e.g. a bare actor
        // that crashed) cannot accumulate a dead key across spawns.
        actors.retain(|_, weak| weak.strong_count() > 0);
        actors.insert(pid.get(), core);
        drop(actors);
        Ok(())
    }

    /// Drops the registration for `pid` only when it is owned by `owner` (called
    /// from the close, finalize, restart, and watcher paths, where the owning
    /// core outlives the dead pid so the strong-count prune in `register` cannot
    /// remove it). The identity check makes removal generation-safe: a reused pid
    /// registered by a different core is never removed by a stale caller.
    pub(super) fn deregister_owned(&self, pid: ParticipantPid, owner: &ActorCore) {
        if let Ok(mut actors) = self.actors.lock() {
            if let Some(registered) = actors.get(&pid.get()) {
                if std::ptr::eq(registered.as_ptr(), owner) {
                    actors.remove(&pid.get());
                }
            }
        }
    }

    /// Drops the registration for `pid` only when its owning core is already
    /// gone. The watcher's fallback when it outlives the core: with no owner
    /// left to identity-check against, a dead `Weak` is itself the proof that
    /// the entry cannot belong to any live conversation.
    pub(super) fn deregister_dead(&self, pid: ParticipantPid) {
        if let Ok(mut actors) = self.actors.lock() {
            if let Some(registered) = actors.get(&pid.get()) {
                if registered.strong_count() == 0 {
                    actors.remove(&pid.get());
                }
            }
        }
    }

    /// Number of live actor registrations. Pinned bounded by the churn gate across
    /// open/close cycles.
    pub(super) fn registration_count(&self) -> usize {
        self.actors.lock().map_or(0, |actors| actors.len())
    }

    fn actor(&self, pid: u64) -> Option<Arc<ActorCore>> {
        self.actors
            .lock()
            .ok()
            .and_then(|actors| actors.get(&pid).and_then(Weak::upgrade))
    }
}

pub(super) fn actor_module(
    module_name: Atom,
    entry_function: Atom,
    command_function: Atom,
) -> Module {
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

/// Establishes a beamr process link from the conversation actor to each of its
/// configured participants. Called during boot, before any message is forwarded,
/// so the link delivering EXIT signals exists before the consumer can crash.
///
/// A participant whose process no longer exists cannot be linked; failing boot
/// on it would make the conversation unrestartable after a crash under a
/// non-Fail policy. Instead the dead pid is pruned from linking and its death
/// is routed through the same host-side recording path as a trapped EXIT
/// ([`ActorCore::record_participant_exit`]) — a no-op when the crash was
/// already recorded before the previous actor process died, so nothing is
/// double-recorded or double-signalled. This mirrors the channel actor's boot,
/// which prunes dead subscribers rather than failing.
pub(super) fn link_participants(
    core: &ActorCore,
    context: &ProcessContext<'_>,
) -> Result<(), LiminalError> {
    let actor_pid = context
        .pid()
        .ok_or_else(|| LiminalError::ConversationFailed {
            message: "conversation actor has no beamr pid".to_owned(),
        })?;
    let link_facility =
        context
            .link_facility()
            .ok_or_else(|| LiminalError::ConversationFailed {
                message: "beamr link facility is unavailable".to_owned(),
            })?;
    for participant in &core.config.participants {
        if !core.participant_process_is_live(*participant) {
            // Boot-discovered death: no EXIT signal was observed, so no reason
            // is available to record.
            core.record_participant_exit(*participant, Instant::now(), None)?;
            continue;
        }
        link_facility
            .link(actor_pid, participant.get())
            .map_err(|error| LiminalError::ParticipantCrashed {
                message: format!(
                    "failed to link actor {actor_pid} to participant {}: {error}",
                    participant.get()
                ),
            })?;
    }
    // Link the actor to its exit watcher, completing the arm-then-link ordering
    // the watcher's cleanup depends on (the watcher armed trap-exit before this
    // actor was booted). The watcher is filtered out of participant-EXIT
    // handling by the config-membership check in the command NIF.
    if let Some(watcher) = core.watcher_pid() {
        link_facility
            .link(actor_pid, watcher.get())
            .map_err(|error| LiminalError::ConversationFailed {
                message: format!(
                    "failed to link actor {actor_pid} to exit watcher {}: {error}",
                    watcher.get()
                ),
            })?;
    }
    Ok(())
}

fn process_command_nif(args: &[Term], context: &mut ProcessContext<'_>) -> Result<Term, Term> {
    let [message] = args else {
        return Err(badarg());
    };
    let pid = context.pid().ok_or_else(badarg)?;
    let runtime = runtime(context)?;
    let core = runtime.actor(pid).ok_or_else(badarg)?;
    if message.as_atom() == Some(runtime.command_atom) {
        return core.process_next_command(context);
    }
    if let Some((source, reason)) = exit_source(*message) {
        // Only configured participants are recorded: the actor is also linked to
        // its exit watcher, whose EXIT (or any other stray link's) must not be
        // recorded as a participant crash or trip the crash policy.
        if core.config.participants.contains(&source) {
            core.handle_participant_exit(source, reason)
                .map_err(|_| badarg())?;
        }
    }
    Ok(Term::atom(Atom::OK))
}

fn runtime(context: &ProcessContext<'_>) -> Result<Arc<ActorRuntime>, Term> {
    let data = context.nif_private_data().ok_or_else(badarg)?;
    Arc::downcast::<ActorRuntime>(Arc::clone(data)).map_err(|_| badarg())
}

/// Parses a trapped `{EXIT, SourcePid, Reason}` tuple into the source pid and
/// the exit reason its reason atom maps to (`None` for a reason term outside
/// beamr's `ExitReason` atom set). Shared by the actor NIF and the exit watcher.
pub(super) fn exit_source(message: Term) -> Option<(ParticipantPid, Option<ExitReason>)> {
    let tuple = Tuple::new(message)?;
    if tuple.arity() == 3 && tuple.get(0) == Some(Term::atom(Atom::EXIT)) {
        let source = tuple.get(1)?.as_pid().map(ParticipantPid::new)?;
        let reason = tuple.get(2).and_then(exit_reason_from_atom);
        Some((source, reason))
    } else {
        None
    }
}

/// Maps an EXIT reason term back to the `ExitReason` it was emitted from
/// (the inverse of `ExitReason::as_term`).
fn exit_reason_from_atom(reason: Term) -> Option<ExitReason> {
    match reason.as_atom()? {
        Atom::NORMAL => Some(ExitReason::Normal),
        Atom::KILL => Some(ExitReason::Kill),
        Atom::KILLED => Some(ExitReason::Killed),
        Atom::ERROR => Some(ExitReason::Error),
        Atom::NOCONNECTION => Some(ExitReason::NoConnection),
        Atom::NOPROC => Some(ExitReason::NoProc),
        _ => None,
    }
}

const fn badarg() -> Term {
    Term::atom(Atom::BADARG)
}
