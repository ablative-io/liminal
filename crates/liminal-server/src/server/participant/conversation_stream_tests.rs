use std::sync::Arc;

use liminal::durability::{DurableStore, open_ephemeral};

use super::conversation_stream::{ConversationEventStream, ConversationStreamError};

fn event_stream() -> Result<ConversationEventStream, Box<dyn std::error::Error>> {
    let store: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    Ok(ConversationEventStream::new(store, 41))
}

#[test]
fn pages_are_contiguous_bounded_and_end_on_empty() -> Result<(), Box<dyn std::error::Error>> {
    let stream = event_stream()?;
    assert_eq!(
        stream.stream_key(),
        "liminal/participant/conversation/v1/41"
    );

    let next = liminal::durability::bridge::block_on(stream.append(0, vec![1]))??;
    let next = liminal::durability::bridge::block_on(stream.append(next, vec![2]))??;
    assert_eq!(next, 2);

    let page = liminal::durability::bridge::block_on(stream.read_page(0))??;
    assert_eq!(page.next_sequence(), 2);
    let entries = page.into_entries();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].payload, vec![1]);
    assert_eq!(entries[1].payload, vec![2]);

    let end = liminal::durability::bridge::block_on(stream.read_page(2))??;
    assert!(end.is_empty());
    assert_eq!(end.next_sequence(), 2);
    Ok(())
}

#[test]
fn optimistic_conflict_is_returned_without_retry() -> Result<(), Box<dyn std::error::Error>> {
    let stream = event_stream()?;
    let _ = liminal::durability::bridge::block_on(stream.append(0, vec![1]))??;

    let result = liminal::durability::bridge::block_on(stream.append(0, vec![2]))?;
    let Err(error) = result else {
        return Err(std::io::Error::other("stale optimistic head unexpectedly appended").into());
    };
    assert!(matches!(error, ConversationStreamError::Durability(_)));

    let page = liminal::durability::bridge::block_on(stream.read_page(0))??;
    let entries = page.into_entries();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].payload, vec![1]);
    Ok(())
}

#[test]
fn append_rejects_unrepresentable_next_sequence_before_storage()
-> Result<(), Box<dyn std::error::Error>> {
    let stream = event_stream()?;
    let result = liminal::durability::bridge::block_on(stream.append(u64::MAX, vec![1]))?;
    let Err(error) = result else {
        return Err(std::io::Error::other("MAX stream head unexpectedly appended").into());
    };
    assert!(matches!(error, ConversationStreamError::SequenceExhausted));

    let empty = liminal::durability::bridge::block_on(stream.read_page(0))??;
    assert!(empty.is_empty());
    Ok(())
}
