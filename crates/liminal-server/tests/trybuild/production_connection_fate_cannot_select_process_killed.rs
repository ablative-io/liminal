use liminal_server::server::participant::ConnectionFateClass;

fn production_binding_emitter(class: ConnectionFateClass) -> bool {
    matches!(class, ConnectionFateClass::ProcessKilled)
}

fn main() {
    let _ = production_binding_emitter;
}
