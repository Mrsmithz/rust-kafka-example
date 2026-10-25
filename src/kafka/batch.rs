use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Mutex,
};

use anyhow::{Result, anyhow, ensure};
use rdkafka::{Message, Offset, TopicPartitionList, message::OwnedMessage};
use tokio_util::sync::CancellationToken;

pub(super) struct Fence {
    state: Mutex<Assignment>,
}
struct Assignment {
    epoch: u64,
    partitions: BTreeSet<i32>,
    cancelled: CancellationToken,
}
#[derive(Clone)]
pub(crate) struct AssignmentToken {
    pub(crate) epoch: u64,
    pub(crate) cancelled: CancellationToken,
}

impl Fence {
    pub(super) fn new() -> Self {
        Self {
            state: Mutex::new(Assignment {
                epoch: 0,
                partitions: BTreeSet::new(),
                cancelled: CancellationToken::new(),
            }),
        }
    }
    pub(super) fn replace(&self, partitions: impl IntoIterator<Item = i32>) -> Result<u64> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow!("assignment lock poisoned"))?;
        state.cancelled.cancel();
        state.epoch = state
            .epoch
            .checked_add(1)
            .ok_or_else(|| anyhow!("assignment epoch exhausted"))?;
        state.partitions = partitions.into_iter().collect();
        state.cancelled = CancellationToken::new();
        Ok(state.epoch)
    }
    pub(super) fn snapshot(&self) -> Result<AssignmentToken> {
        let state = self
            .state
            .lock()
            .map_err(|_| anyhow!("assignment lock poisoned"))?;
        Ok(AssignmentToken {
            epoch: state.epoch,
            cancelled: state.cancelled.clone(),
        })
    }
    pub(super) fn allows(&self, token: &AssignmentToken, offsets: &TopicPartitionList) -> bool {
        self.state.lock().is_ok_and(|state| {
            state.epoch == token.epoch
                && !token.cancelled.is_cancelled()
                && offsets
                    .elements()
                    .iter()
                    .all(|p| state.partitions.contains(&p.partition()))
        })
    }
}

pub(crate) struct Batch {
    pub(crate) messages: Vec<OwnedMessage>,
    pub(crate) assignment: AssignmentToken,
}

pub(super) fn next_offsets(messages: &[OwnedMessage]) -> Result<TopicPartitionList> {
    let mut maximum = BTreeMap::<(&str, i32), i64>::new();
    for message in messages {
        ensure!(message.offset() >= 0, "record has no concrete offset");
        let next = message
            .offset()
            .checked_add(1)
            .ok_or_else(|| anyhow!("offset overflow"))?;
        maximum
            .entry((message.topic(), message.partition()))
            .and_modify(|offset| *offset = (*offset).max(next))
            .or_insert(next);
    }
    let mut offsets = TopicPartitionList::new();
    for ((topic, partition), offset) in maximum {
        offsets.add_partition_offset(topic, partition, Offset::Offset(offset))?;
    }
    Ok(offsets)
}

pub(super) fn message_bytes(message: &impl Message) -> usize {
    use rdkafka::message::Headers;
    let headers = message.headers().map_or(0, |headers| {
        headers.iter().fold(0usize, |sum, header| {
            sum.saturating_add(header.key.len())
                .saturating_add(header.value.map_or(0, <[u8]>::len))
        })
    });
    message
        .key()
        .map_or(0, <[u8]>::len)
        .saturating_add(message.payload().map_or(0, <[u8]>::len))
        .saturating_add(headers)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use rdkafka::message::Timestamp;

    fn message(partition: i32, offset: i64) -> OwnedMessage {
        OwnedMessage::new(
            None,
            None,
            "topic".into(),
            Timestamp::NotAvailable,
            partition,
            offset,
            None,
        )
    }
    #[test]
    fn offset_gaps_and_completion_order_do_not_require_a_numeric_ledger() -> Result<()> {
        let offsets = next_offsets(&[message(0, 7), message(1, 3), message(0, 2), message(0, 10)])?;
        assert_eq!(
            offsets.find_partition("topic", 0).map(|p| p.offset()),
            Some(Offset::Offset(11))
        );
        assert_eq!(
            offsets.find_partition("topic", 1).map(|p| p.offset()),
            Some(Offset::Offset(4))
        );
        Ok(())
    }
    #[test]
    fn same_partition_reassignment_never_reuses_the_old_token() -> Result<()> {
        let fence = Fence::new();
        fence.replace([0])?;
        let old = fence.snapshot()?;
        let offsets = next_offsets(&[message(0, 4)])?;
        assert!(fence.allows(&old, &offsets));
        fence.replace([])?;
        fence.replace([0])?;
        assert!(old.cancelled.is_cancelled());
        assert!(!fence.allows(&old, &offsets));
        assert!(fence.allows(&fence.snapshot()?, &offsets));
        Ok(())
    }
    #[test]
    fn invalid_or_overflowing_offsets_are_rejected() {
        assert!(next_offsets(&[message(0, -1)]).is_err());
        assert!(next_offsets(&[message(0, i64::MAX)]).is_err());
    }
    proptest! {
        #[test]
        fn commits_only_one_past_the_highest_delivered_offset(mut values in prop::collection::vec(0i64..1_000_000, 1..128)) {
            let expected = values.iter().max().copied().unwrap_or(0) + 1;
            values.reverse();
            let records: Vec<_> = values.into_iter().map(|offset| message(0, offset)).collect();
            let actual = next_offsets(&records).map(|list| list.find_partition("topic", 0).map(|p| p.offset()));
            prop_assert_eq!(actual.ok(), Some(Some(Offset::Offset(expected))));
        }
    }
}
