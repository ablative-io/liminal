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
        lock(&self.actors, "actor runtime")?.insert(pid.get(), core);
        Ok(())
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
        link_facility
            .link(actor_pid, participant.get())
            .map_err(|error| LiminalError::ParticipantCrashed {
                message: format!(
                    "failed to link actor {actor_pid} to participant {}: {error}",
                    participant.get()
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
    if let Some(participant) = exit_source(*message) {
        core.handle_participant_exit(participant)
            .map_err(|_| badarg())?;
    }
    Ok(Term::atom(Atom::OK))
}

fn runtime(context: &ProcessContext<'_>) -> Result<Arc<ActorRuntime>, Term> {
    let data = context.nif_private_data().ok_or_else(badarg)?;
    Arc::downcast::<ActorRuntime>(Arc::clone(data)).map_err(|_| badarg())
}

fn exit_source(message: Term) -> Option<ParticipantPid> {
    let tuple = Tuple::new(message)?;
    if tuple.arity() == 3 && tuple.get(0) == Some(Term::atom(Atom::EXIT)) {
        tuple.get(1)?.as_pid().map(ParticipantPid::new)
    } else {
        None
    }
}

const fn badarg() -> Term {
    Term::atom(Atom::BADARG)
}
