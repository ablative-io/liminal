use std::error::Error;

use beamr::process::ExitReason;

use super::super::codec::{DispatchRequest, encode_dispatch_request};
use super::*;
use crate::aion::types::Payload;

#[test]
fn registration_opens_a_real_subscription() -> Result<(), Box<dyn Error>> {
    let context = WorkerContext::new();
    let registration = reg(&context, "email", "worker-a", 4, &["send-email"])?;
    let channel = dispatch_channel("prod", "email")?;
    let workers = context.workers_for_channel(&channel, &request("send-email"))?;
    assert_eq!(worker_ids(&workers), vec!["worker-a"]);
    assert_eq!(workers[0].consumer_state.max_in_flight, 4);
    assert_eq!(workers[0].consumer_state.affinity_tags, vec!["send-email"]);

    let encoded = encode_dispatch_request(&DispatchRequest::new(
        "conversation-1".to_owned(),
        request("send-email"),
    ))?;
    context.session_for(&channel)?.handle.publish(encoded)?;
    let delivered = registration.try_next()?;
    assert!(delivered.is_some());
    Ok(())
}

#[test]
fn liveness_and_capacity_are_reflected_in_snapshots() -> Result<(), Box<dyn Error>> {
    let context = WorkerContext::new();
    let channel = dispatch_channel("prod", "email")?;
    let first = reg(&context, "email", "worker-a", 2, &["send-email"])?;
    let second = reg(&context, "email", "worker-b", 3, &["send-email", "sms"])?;
    second.set_in_flight(3);
    let workers = context.workers_for_channel(&channel, &request("send-email"))?;
    assert_eq!(worker_ids(&workers), vec!["worker-a", "worker-b"]);
    assert!(!workers[1].consumer_state.has_capacity());

    first.unregister()?;
    let remaining = context.workers_for_channel(&channel, &request("send-email"))?;
    assert_eq!(worker_ids(&remaining), vec!["worker-b"]);
    assert_eq!(remaining[0].consumer_state.current_in_flight, 3);
    drop(second);
    assert!(
        context
            .workers_for_channel(&channel, &request("send-email"))?
            .is_empty()
    );
    Ok(())
}

#[test]
fn repeated_registrations_change_the_next_snapshot() -> Result<(), Box<dyn Error>> {
    let context = WorkerContext::new();
    let channel = dispatch_channel("prod", "bulk")?;
    let mut registrations = Vec::new();
    for index in 0_u64..3 {
        let worker_id = format!("worker-{index}");
        registrations.push(reg(&context, "bulk", &worker_id, 1, &["bulk"])?);
        assert_eq!(
            context
                .workers_for_channel(&channel, &request("bulk"))?
                .len(),
            registrations.len()
        );
    }
    registrations.remove(1).unregister()?;
    let workers = context.workers_for_channel(&channel, &request("bulk"))?;
    assert_eq!(worker_ids(&workers), vec!["worker-0", "worker-2"]);
    Ok(())
}

#[test]
fn crashed_worker_is_removed_by_process_link_without_affecting_survivors()
-> Result<(), Box<dyn Error>> {
    let context = WorkerContext::new();
    let channel = dispatch_channel("prod", "email")?;
    let crashed = reg(&context, "email", "crashed", 1, &["send-email"])?;
    let survivor = reg(&context, "email", "survivor", 1, &["send-email"])?;
    assert_eq!(
        worker_ids(&context.workers_for_channel(&channel, &request("send-email"))?),
        vec!["crashed", "survivor"]
    );

    context
        .supervisor_for(&channel)?
        .scheduler()
        .terminate_process(crashed.participant().get(), ExitReason::Error);

    for _ in 0..1_000 {
        let workers = context.workers_for_channel(&channel, &request("send-email"))?;
        if worker_ids(&workers) == vec!["survivor"] {
            assert_eq!(survivor.worker_id(), "survivor");
            return Ok(());
        }
        std::thread::yield_now();
    }

    Err("crashed worker remained dispatch-eligible after link notification".into())
}

fn reg(
    context: &WorkerContext,
    queue: &str,
    worker_id: &str,
    max: usize,
    tags: &[&str],
) -> Result<WorkerRegistration, AionSurfaceError> {
    let channel = dispatch_channel("prod", queue)?;
    let participant = link::spawn_worker_process(context, &channel)?;
    context.register_worker_with_participant(
        "prod",
        queue,
        worker_id,
        participant,
        capacity(max, tags),
    )
}

fn capacity(max_concurrent: usize, activity_types: &[&str]) -> WorkerCapacity {
    WorkerCapacity {
        max_concurrent,
        activity_types: activity_types
            .iter()
            .map(|activity| (*activity).to_owned())
            .collect(),
    }
}

fn request(activity_type: &str) -> ActivityRequest {
    ActivityRequest {
        activity_type: activity_type.to_owned(),
        input: Payload::default(),
        task_queue: "email".to_owned(),
        schedule_to_close_timeout: None,
        start_to_close_timeout: None,
    }
}

fn worker_ids(workers: &[DispatchWorker]) -> Vec<&str> {
    workers
        .iter()
        .map(|worker| worker.worker_id.as_str())
        .collect()
}
