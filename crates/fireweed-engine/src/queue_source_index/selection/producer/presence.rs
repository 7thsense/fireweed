//! Exact field presence for a particular actual administrative selector node.
use super::*;
use crate::queue_source_index::selection::scalar::{
    plan_source_presence_ranges, presence_plan_capacity,
};

impl<'a> QueueSourceSelectionRequest<'a> {
    fn presence(self, clause: usize, node: usize) -> QueueResult<(&'a str, bool)> {
        let Self::Administration { intent, .. } = self else {
            return Err(unknown("presence occurrence differs from actual request"));
        };
        let AdministrativeOperation::SelectFirst(clauses) = &intent.operation else {
            return Err(unknown("presence occurrence differs from actual request"));
        };
        match clauses
            .get(clause)
            .and_then(|clause| clause.selector.nodes.get(node))
        {
            Some(SelectorNode::Present { name, present }) => Ok((name, *present)),
            _ => Err(unknown("presence occurrence differs from actual request")),
        }
    }
}
impl<'a, R: QueueSourceStreamingReader + Send, S: QueueSourceCandidateScratch>
    QueueSourceCandidateProducer<'a, R, S>
{
    /// Produces field presence, including for a Missing node. The Boolean
    /// assembler must derive Missing as U minus this complete presence set.
    pub async fn presence(
        mut self,
        clause: usize,
        node: usize,
    ) -> QueueResult<(Self, QueueSourcePresenceLeaf<'a, R::Completion, S::Set>)> {
        self.validate()?;
        let (field, present) = self._request.presence(clause, node)?;
        let family = crate::queue_source_index::QueueSourcePostingFamily::Scalar {
            collection: None,
            field,
        };
        self.budget.grant(
            &self.job,
            QueueSourceSelectionWork {
                copied_bytes: presence_plan_capacity(family)?,
                ..Default::default()
            },
        )?;
        let plan = plan_source_presence_ranges(self.definition, None, field, &self.job)?;
        let slots = plan
            .ranges()
            .len()
            .checked_mul(std::mem::size_of::<R::Completion>())
            .and_then(|n| {
                n.checked_add(std::mem::size_of::<
                    QueueSourcePresenceLeaf<'a, R::Completion, S::Set>,
                >())
            })
            .and_then(|n| n.checked_add(2048))
            .ok_or(QueueError::TooLarge)?;
        let memory = self.job.owned_buffer(slots as u64)?;
        let (completions, set) = self.scan_ranges(&plan).await?;
        let leaf = QueueSourcePresenceLeaf {
            plan,
            completions,
            set,
            identity: self.identity.clone(),
            clause,
            node,
            present,
            _memory: memory,
        };
        self.validate_presence(&leaf, clause, node)?;
        Ok((self, leaf))
    }
    pub fn validate_presence(
        &self,
        leaf: &QueueSourcePresenceLeaf<'_, R::Completion, S::Set>,
        clause: usize,
        node: usize,
    ) -> QueueResult<()> {
        self.validate()?;
        let (field, present) = self._request.presence(clause, node)?;
        let crate::queue_source_index::QueueSourcePostingFamily::Scalar {
            collection,
            field: planned,
        } = leaf.plan.family()
        else {
            return Err(unknown("presence family differs"));
        };
        if !Arc::ptr_eq(&leaf.identity, &self.identity)
            || leaf.clause != clause
            || leaf.node != node
            || leaf.present != present
            || collection.is_some()
            || planned != field
            || leaf.plan.definition_pin() != self.definition.pin()
            || !leaf.plan.belongs_to(&self.job)
            || leaf.completions.len() != leaf.plan.ranges().len()
            || leaf.completions.is_empty()
        {
            return Err(unknown("presence binding or complete manifest differs"));
        }
        for (range, proof) in leaf.plan.ranges().iter().zip(&leaf.completions) {
            self.reader
                .validate_stream_completion(proof, range.lower(), range.upper())?;
        }
        Ok(())
    }
    /// Complete physical presence cardinality, not the requested Boolean result.
    pub fn presence_len(
        &self,
        leaf: &QueueSourcePresenceLeaf<'_, R::Completion, S::Set>,
        clause: usize,
        node: usize,
    ) -> QueueResult<u64> {
        self.validate_presence(leaf, clause, node)?;
        self.scratch.count(&leaf.set)
    }
}
