//! One fail-closed command-coverage rule for every provisional reducer.
use super::*;
use crate::queue_publication::IdentityRef;

pub(super) struct CommandCoverage<'c, 'a> {
    owner: &'c OwnedQueueBufferCharge,
    expected: u64,
    processed: u64,
    last: Option<IdentityRef<'a>>,
    poisoned: bool,
}
impl<'c, 'a> CommandCoverage<'c, 'a> {
    pub fn new(owner: &'c OwnedQueueBufferCharge, expected: u64) -> Self {
        Self {
            owner,
            expected,
            processed: 0,
            last: None,
            poisoned: false,
        }
    }
    pub fn begin(
        &mut self,
        transition: &QueueSourceRecordTransition<'_, 'a>,
    ) -> QueueResult<IdentityRef<'a>> {
        if self.poisoned {
            return Err(unknown("source command accumulator poisoned"));
        }
        self.poisoned = true;
        let identity = transition.publication_identity();
        if !std::ptr::eq(transition.context_owner(), self.owner)
            || self.processed >= self.expected
            || self.last.is_some_and(|last| {
                (last.collection, last.key) >= (identity.collection, identity.key)
            })
        {
            return Err(unknown("source command coverage differs"));
        }
        Ok(identity)
    }
    pub fn complete(&mut self, identity: IdentityRef<'a>) -> QueueResult<()> {
        self.processed = self.processed.checked_add(1).ok_or(QueueError::TooLarge)?;
        self.last = Some(identity);
        self.poisoned = false;
        Ok(())
    }
    pub fn verify(&self) -> QueueResult<()> {
        if self.poisoned || self.processed != self.expected {
            return Err(unknown("incomplete source command coverage"));
        }
        Ok(())
    }
}
