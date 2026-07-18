use super::*;

pub(super) fn base_rows(
    store: Arc<dyn DurableStore>,
    conversation_id: u64,
) -> Result<Vec<(u64, StoredOperation)>, Box<dyn Error>> {
    let log = OperationLog::new(store, conversation_id);
    let mut rows = Vec::new();
    let mut next = 0;
    loop {
        let page = block_on(log.read_page(next))??;
        if page.is_empty() {
            break;
        }
        next = page
            .last()
            .map(|(sequence, _)| sequence.saturating_add(1))
            .ok_or("nonempty base page lost its tail")?;
        rows.extend(page);
    }
    Ok(rows)
}

pub(super) fn extension_rows(
    store: Arc<dyn DurableStore>,
    conversation_id: u64,
) -> Result<Vec<(u64, OutboxRow)>, Box<dyn Error>> {
    Ok(block_on(
        OutboxLog::new(store, conversation_id).read_all(),
    )??)
}
