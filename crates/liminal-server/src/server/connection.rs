#[path = "connection/apply.rs"]
mod apply;
#[path = "connection/conversation.rs"]
mod conversation;
#[path = "connection/delivery.rs"]
mod delivery;
#[path = "connection/incarnation.rs"]
mod incarnation;
#[cfg(test)]
#[path = "connection/incarnation_tests.rs"]
mod incarnation_tests;
#[path = "connection/notifier.rs"]
pub mod notifier;
#[path = "connection/outbound.rs"]
mod outbound;
#[path = "connection/participant_delivery.rs"]
mod participant_delivery;
#[cfg(test)]
#[path = "connection/participant_runtime_tests.rs"]
mod participant_runtime_tests;
#[path = "connection/pending_reply.rs"]
mod pending_reply;
#[path = "connection/process.rs"]
mod process;
#[path = "connection/services.rs"]
pub mod services;
#[path = "connection/services_cluster.rs"]
mod services_cluster;
#[cfg(test)]
#[path = "connection/services_r5_tests.rs"]
mod services_r5_tests;
#[path = "connection/services_schema.rs"]
mod services_schema;
#[path = "connection/state.rs"]
mod state;
#[path = "connection/supervisor.rs"]
mod supervisor;
#[path = "connection/wake.rs"]
pub(crate) mod wake;
#[path = "connection/websocket.rs"]
mod websocket;
#[path = "connection/worker_front_door.rs"]
mod worker_front_door;

pub use conversation::{ConnectionConversation, ConversationResource};
pub use notifier::ConnectionNotifier;
#[cfg(test)]
pub(crate) const fn assert_held_heads_are_move_only() {
    macro_rules! assert_not_impl {
        ($type:ty: $trait:path) => {
            const _: fn() = || {
                struct Probe<T: ?Sized>(core::marker::PhantomData<T>);
                trait AmbiguousIfImplemented<A> {
                    fn probe() {}
                }
                impl<T: ?Sized> AmbiguousIfImplemented<()> for Probe<T> {}
                impl<T: ?Sized + $trait> AmbiguousIfImplemented<u8> for Probe<T> {}
                let _ = <Probe<$type> as AmbiguousIfImplemented<_>>::probe;
            };
        };
    }
    assert_not_impl!(participant_delivery::HeldParticipantHead: Clone);
    assert_not_impl!(participant_delivery::HeldParticipantHead: Copy);
    assert_not_impl!(participant_delivery::HeldObserverHead: Clone);
    assert_not_impl!(participant_delivery::HeldObserverHead: Copy);
}
pub use services::{
    ChannelCluster, ConnectionServices, ConnectionSubscription, LiminalConnectionServices,
    PublishOutcome, build_connection_services,
};
pub use supervisor::{ConnectionHandle, ConnectionSupervisor, PushReplyAwaiter};
pub(crate) use wake::ReadyWaker;
pub use websocket::WebSocketListener;
pub use worker_front_door::WorkerFrontDoorServices;
