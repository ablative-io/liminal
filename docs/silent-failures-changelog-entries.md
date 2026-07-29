# Silent-failures branch — staged changelog entries

Ready-to-paste bullets for the landing owner to append under the `## Unreleased`
section held on another branch. Groups map to the usual `### Added` /
`### Fixed` / `### Changed` headings; no hashes, no dates.

## New warn/error logging (`### Added`, or fold into `### Fixed`)

- **A typo'd `LIMINAL_` environment key is now named at warn level.** The key match's catch-all arm still ignores the key and configuration still loads; the operator finally learns that the setting they wrote is doing nothing.
- **Stray and server-to-client-only inbound frames warn with the frame kind.** The catch-all no-response arm named neither the frame nor the fact that it was dropped; the no-teardown semantics are unchanged.
- **A channel that declares a `schema_ref` but resolves permissive now warns, naming the channel.** A schema-less channel stays silent and resolution behaviour is unchanged, so this only exposes the declared-but-unloaded drift that was silently downgrading validation.
- **Server-metrics family registration failures warn with the registration error.** The families that failed to register previously disappeared, leaving a metrics endpoint that was quietly incomplete; a missing global registry at init is named too.
- **A poisoned shutdown-wait lock is logged at error, and `notify` still wakes parked waiters through it.** The notify runs under the recovered guard so a waiter cannot be lost in the window between its `is_initiated` check and its park; the signal-worker join failure in `Drop` is raised from debug to warn.
- **Cluster membership consumer failures are named.** A consumer-thread spawn failure now reaches an error log instead of being discarded by `.ok()` (the cluster still runs on), and a consumer thread that panicked is warned about when `stop()` joins it.
- **Cross-node distribution write failures warn with the io error.** The shared blocking write behind both the fan-out and the R5 backfill paths discarded every `Err`; write semantics are unchanged.
- **The connection reclamation reactor names the reason it stops.** Losing its exit-event subscription mid-flight ends the only crash-reclamation source for a process that dies without a final slice, and it now warns at the same volume as the startup branch that finds no subscription at all.
- **Partial activation and unencodable rejections say so.** Both partial-activation arms warn with the stage and which half of the invariant is present, and the rejection-encode fallback no longer discards its codec error — the reason a client was rejected existed nowhere before.
- **Listener and health lifecycle failures are named.** A failed self-connect interrupt (the only thing that can wake a worker parked in `accept`) warns with its target and error instead of turning into a silent shutdown deadlock; the four `try_clone().ok()` calls that silently disabled the shed-with-spare-fd policy now route through one named reserve-descriptor helper; and a failed admitted-slot clone in the health endpoint no longer leaves `stop_worker` with nothing to interrupt.
- **An undelivered participant `READY` wake is reported.** Both fire sites name which publication kind lost its wake; the publication is still queued and both paths still succeed, but a genuinely stranded publication is no longer indistinguishable from the benign teardown case.

## Behaviour fixes (`### Fixed`)

- **A vanished participant actor now reads as crashed, not as healthy.** A disconnected participant-EXIT channel means the actor that owned the sender is gone, so it is treated as a crash observation on both the polling and the blocking leg — previously it read as "no crash", which let a connection keep forwarding conversation messages into a participant that no longer existed, and made the blocking leg burn its full timeout first. An actor that cannot service a state query is likewise reported as failed rather than healthy.
- **`peer_names` renders an unresolvable peer atom instead of dropping it.** Peers now go through the same `<atom N>` fallback the rest of the module uses, so the two accessors can no longer disagree and a log line built from `peer_names` can no longer under-report the cluster.
- **A failed post-failure repair is composed into the reported error.** When an operation fails with a committed prefix staged and the replay-and-repair that follows also fails, the repair error is now carried alongside the operation error instead of being discarded, so the reason durable state could not be reconciled reaches the caller.

## Caller-visible text (`### Changed`)

- **`push_to_connection` no longer blames a live process for a refused control queue.** Its refusal message changes from `process is not live` to `the control was not published`, which is the outcome this path can actually observe; the underlying cause is warned about where it is known.
