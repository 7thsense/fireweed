//! Exact actual-request key points, including authenticated nonmembership.
use super::*;
use std::collections::BTreeSet;
impl<'a> QueueSourceSelectionRequest<'a> {
    fn keys(self, clause: usize, node: usize) -> QueueResult<&'a BTreeSet<String>> {
        let (program, _) = self.boolean_program(clause)?;
        match program.nodes.get(node) {
            Some(SelectorNode::Keys(keys)) => Ok(keys),
            _ => Err(unknown("key occurrence differs from actual request")),
        }
    }
}
impl<'a, R: QueueSourceStreamingReader + Send, S: QueueSourceCandidateScratch>
    QueueSourceCandidateProducer<'a, R, S>
{
    pub async fn keys(
        mut self,
        clause: usize,
        node: usize,
    ) -> QueueResult<(Self, QueueSourceKeysLeaf<'a, S::Set>)> {
        self.validate()?;
        let keys = self._request.keys(clause, node)?;
        let capacity = keys
            .len()
            .checked_mul(std::mem::size_of::<KeyObservation<'a>>())
            .and_then(|n| n.checked_add(std::mem::size_of::<QueueSourceKeysLeaf<'a, S::Set>>()))
            .and_then(|n| n.checked_add(1024))
            .ok_or(QueueError::TooLarge)?;
        let memory = self.job.owned_buffer(capacity as u64)?;
        self.budget.grant(
            &self.job,
            QueueSourceSelectionWork {
                rows: keys.len() as u64,
                copied_bytes: capacity as u64,
                scratch_steps: 0,
            },
        )?;
        let mut observations = Vec::with_capacity(keys.len());
        let mut builder = self.scratch.begin_set()?;
        for key in keys {
            let record = record::record_at(
                &self.reader,
                self.original.cut(),
                &self.header,
                None,
                key,
                &self.job,
                &self.budget,
            )
            .await?;
            let live = record.is_some_and(|record| record.live.is_some());
            // The full identity comes from an authenticated live Record point,
            // never from an unchecked requested key or a truncated candidate.
            if live {
                self.scratch.insert(&mut builder, None, key).await?;
            }
            observations.push(KeyObservation {
                key,
                highwater: record.map(|record| record.highwater),
                live,
            });
        }
        let set = self.scratch.seal(builder)?;
        let leaf = QueueSourceKeysLeaf {
            observations,
            set,
            identity: self.identity.clone(),
            clause,
            node,
            _memory: memory,
        };
        self.validate_keys(&leaf, clause, node)?;
        Ok((self, leaf))
    }
    pub fn validate_keys(
        &self,
        leaf: &QueueSourceKeysLeaf<'_, S::Set>,
        clause: usize,
        node: usize,
    ) -> QueueResult<()> {
        self.validate()?;
        let keys = self._request.keys(clause, node)?;
        if !Arc::ptr_eq(&leaf.identity, &self.identity)
            || leaf.clause != clause
            || leaf.node != node
            || leaf.observations.len() != keys.len()
        {
            return Err(unknown("key binding or complete point manifest differs"));
        }
        for (key, point) in keys.iter().zip(&leaf.observations) {
            if key != point.key
                || point.highwater == Some(0)
                || (point.live && point.highwater.is_none())
            {
                return Err(unknown("key point manifest differs from actual request"));
            }
        }
        Ok(())
    }
    pub fn keys_len(
        &self,
        leaf: &QueueSourceKeysLeaf<'_, S::Set>,
        clause: usize,
        node: usize,
    ) -> QueueResult<u64> {
        self.validate_keys(leaf, clause, node)?;
        self.scratch.count(&leaf.set)
    }
}
