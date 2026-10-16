//! Private actual-program assembly. No caller-supplied plan or attached proofs.
use super::*;
use fireweed_core::queue_administration::WorkSelector;

impl<'a> QueueSourceSelectionRequest<'a> {
    pub(super) fn boolean_program(self, clause: usize) -> QueueResult<(&'a WorkSelector, usize)> {
        let Self::Administration { intent, .. } = self else {
            return Err(unknown("Boolean clause differs from actual request"));
        };
        let AdministrativeOperation::SelectFirst(clauses) = &intent.operation else {
            return Err(unknown("Boolean clause differs from actual request"));
        };
        let selector = &clauses
            .get(clause)
            .ok_or_else(|| unknown("Boolean clause is absent"))?
            .selector;
        let offset = clauses[..clause]
            .iter()
            .flat_map(|clause| &clause.selector.nodes)
            .filter(|node| matches!(node, SelectorNode::Field { .. }))
            .count();
        Ok((selector, offset))
    }
}
fn requirements(program: &WorkSelector) -> QueueResult<(bool, bool)> {
    let (mut universe, mut empty) = (false, false);
    for node in &program.nodes {
        match node {
            SelectorNode::Field { .. }
            | SelectorNode::Keys(_)
            | SelectorNode::Present { present: true, .. } => {}
            SelectorNode::Present { present: false, .. }
            | SelectorNode::Not
            | SelectorNode::All(0) => universe = true,
            SelectorNode::Any(0) => empty = true,
            SelectorNode::All(_) | SelectorNode::Any(_) => {}
            // The whole-program preflight must precede ALL leaf dispatch. The
            // remaining exact intrinsic/time leaves are separate work; an
            // unsupported node can never stand in for an empty completed set.
            _ => return Err(unknown("Boolean clause contains an unsupported leaf")),
        }
    }
    Ok((universe, empty))
}
fn resolved<'b, C, Set>(
    operand: &'b CandidateOperand<Set>,
    clause: &'b QueueSourceBooleanClause<'_, C, Set>,
) -> QueueResult<&'b Set> {
    match operand {
        CandidateOperand::Value(index) => clause.values.get(*index).map(|leaf| &leaf.set),
        CandidateOperand::Presence(index) => clause.presence.get(*index).map(|leaf| &leaf.set),
        CandidateOperand::Keys(index) => clause.keys.get(*index).map(|leaf| &leaf.set),
        CandidateOperand::Universe => clause.universe.as_ref().map(|leaf| &leaf.set),
        CandidateOperand::Empty => clause.empty.as_ref(),
        CandidateOperand::Owned(set) => Some(set),
    }
    .ok_or_else(|| unknown("Boolean operand has no completed set"))
}
impl<'a, R: QueueSourceStreamingReader + Send, S: QueueSourceCandidateScratch>
    QueueSourceCandidateProducer<'a, R, S>
{
    pub async fn boolean_clause(
        mut self,
        clause: usize,
    ) -> QueueResult<(Self, QueueSourceBooleanClause<'a, R::Completion, S::Set>)> {
        self.validate()?;
        let (program, offset) = self._request.boolean_program(clause)?;
        // Pure, complete support preflight; no range or set is dispatched/sealed
        // until even a logically decisive program has been checked in full.
        let (needs_universe, needs_empty) = requirements(program)?;
        let count = program.nodes.len();
        let per_node = std::mem::size_of::<CandidateOperand<S::Set>>()
            .checked_add(std::mem::size_of::<
                QueueSourceScalarLeaf<'a, R::Completion, S::Set>,
            >())
            .and_then(|n| {
                n.checked_add(std::mem::size_of::<
                    QueueSourcePresenceLeaf<'a, R::Completion, S::Set>,
                >())
            })
            .and_then(|n| n.checked_add(std::mem::size_of::<QueueSourceKeysLeaf<'a, S::Set>>()))
            .ok_or(QueueError::TooLarge)?;
        let temporary_capacity = std::mem::size_of::<CandidateOperand<S::Set>>()
            .checked_mul(4)
            .ok_or(QueueError::TooLarge)?;
        let capacity = count
            .checked_mul(per_node)
            .and_then(|n| n.checked_add(temporary_capacity))
            .and_then(|n| {
                n.checked_add(std::mem::size_of::<
                    QueueSourceBooleanClause<'a, R::Completion, S::Set>,
                >())
            })
            .and_then(|n| n.checked_add(2048))
            .ok_or(QueueError::TooLarge)?;
        let memory = self.job.owned_buffer(capacity as u64)?;
        self.budget.grant(
            &self.job,
            QueueSourceSelectionWork {
                rows: (count as u64).checked_mul(4).ok_or(QueueError::TooLarge)?,
                copied_bytes: capacity as u64,
                scratch_steps: 0,
            },
        )?;
        let mut proof = QueueSourceBooleanClause {
            result: CandidateOperand::Empty,
            values: Vec::with_capacity(count),
            presence: Vec::with_capacity(count),
            keys: Vec::with_capacity(count),
            universe: None,
            empty: None,
            identity: self.identity.clone(),
            clause,
            _memory: memory,
        };
        let mut stack = Vec::with_capacity(count);
        if needs_universe {
            let (producer, universe) = self.universe().await?;
            self = producer;
            proof.universe = Some(universe);
        }
        if needs_empty {
            let builder = self.scratch.begin_set()?;
            proof.empty = Some(self.scratch.seal(builder)?);
        }
        let mut occurrence = offset;
        for (node_index, node) in program.nodes.iter().enumerate() {
            let value = match node {
                SelectorNode::Field { .. } => {
                    let (producer, leaf) = self.scalar(occurrence).await?;
                    self = producer;
                    occurrence += 1;
                    let index = proof.values.len();
                    proof.values.push(leaf);
                    CandidateOperand::Value(index)
                }
                SelectorNode::Keys(_) => {
                    let (producer, leaf) = self.keys(clause, node_index).await?;
                    self = producer;
                    let index = proof.keys.len();
                    proof.keys.push(leaf);
                    CandidateOperand::Keys(index)
                }
                SelectorNode::Present { present, .. } => {
                    let (producer, leaf) = self.presence(clause, node_index).await?;
                    self = producer;
                    let index = proof.presence.len();
                    proof.presence.push(leaf);
                    let operand = CandidateOperand::Presence(index);
                    if *present {
                        operand
                    } else {
                        let universe = proof
                            .universe
                            .as_ref()
                            .ok_or_else(|| unknown("Missing requires the complete universe"))?;
                        CandidateOperand::Owned(
                            self.scratch
                                .difference(&universe.set, resolved(&operand, &proof)?)
                                .await?,
                        )
                    }
                }
                SelectorNode::Not => {
                    let operand = stack
                        .pop()
                        .ok_or_else(|| unknown("Boolean Not has no operand"))?;
                    let universe = proof
                        .universe
                        .as_ref()
                        .ok_or_else(|| unknown("Not requires the complete universe"))?;
                    CandidateOperand::Owned(
                        self.scratch
                            .difference(&universe.set, resolved(&operand, &proof)?)
                            .await?,
                    )
                }
                SelectorNode::All(arity) | SelectorNode::Any(arity) => {
                    if *arity == 0 {
                        if matches!(node, SelectorNode::All(_)) {
                            CandidateOperand::Universe
                        } else {
                            CandidateOperand::Empty
                        }
                    } else {
                        let mut operand = stack
                            .pop()
                            .ok_or_else(|| unknown("Boolean fold has no operand"))?;
                        for _ in 1..*arity {
                            let next = stack
                                .pop()
                                .ok_or_else(|| unknown("Boolean fold arity differs"))?;
                            let left = resolved(&operand, &proof)?;
                            let right = resolved(&next, &proof)?;
                            let set = if matches!(node, SelectorNode::All(_)) {
                                self.scratch.intersection(left, right).await?
                            } else {
                                self.scratch.union(left, right).await?
                            };
                            operand = CandidateOperand::Owned(set);
                        }
                        operand
                    }
                }
                _ => return Err(unknown("unsupported Boolean leaf escaped preflight")),
            };
            stack.push(value);
        }
        proof.result = stack
            .pop()
            .ok_or_else(|| unknown("Boolean program has no root"))?;
        if !stack.is_empty() {
            return Err(unknown("Boolean program has disconnected roots"));
        }
        self.validate_boolean_clause(&proof, clause)?;
        Ok((self, proof))
    }
    pub fn validate_boolean_clause(
        &self,
        proof: &QueueSourceBooleanClause<'_, R::Completion, S::Set>,
        clause: usize,
    ) -> QueueResult<()> {
        self.validate()?;
        if !Arc::ptr_eq(&proof.identity, &self.identity) || proof.clause != clause {
            return Err(unknown("Boolean producer or clause differs"));
        }
        let (program, mut occurrence) = self._request.boolean_program(clause)?;
        let (universe, empty) = requirements(program)?;
        if universe != proof.universe.is_some() || empty != proof.empty.is_some() {
            return Err(unknown("Boolean constant coverage differs"));
        }
        if let Some(universe) = &proof.universe {
            self.validate_universe(universe)?;
        }
        let (mut values, mut presence, mut keys) = (0, 0, 0);
        for (node, declaration) in program.nodes.iter().enumerate() {
            match declaration {
                SelectorNode::Field { .. } => {
                    self.validate_scalar(
                        proof
                            .values
                            .get(values)
                            .ok_or_else(|| unknown("Boolean value proof missing"))?,
                        occurrence,
                    )?;
                    values += 1;
                    occurrence += 1;
                }
                SelectorNode::Keys(_) => {
                    self.validate_keys(
                        proof
                            .keys
                            .get(keys)
                            .ok_or_else(|| unknown("Boolean key proof missing"))?,
                        clause,
                        node,
                    )?;
                    keys += 1;
                }
                SelectorNode::Present { .. } => {
                    self.validate_presence(
                        proof
                            .presence
                            .get(presence)
                            .ok_or_else(|| unknown("Boolean presence proof missing"))?,
                        clause,
                        node,
                    )?;
                    presence += 1;
                }
                _ => {}
            }
        }
        if values != proof.values.len()
            || presence != proof.presence.len()
            || keys != proof.keys.len()
        {
            return Err(unknown("Boolean manifest has extra occurrences"));
        }
        self.scratch.count(resolved(&proof.result, proof)?)?;
        Ok(())
    }
    pub fn boolean_clause_len(
        &self,
        proof: &QueueSourceBooleanClause<'_, R::Completion, S::Set>,
        clause: usize,
    ) -> QueueResult<u64> {
        self.validate_boolean_clause(proof, clause)?;
        self.scratch.count(resolved(&proof.result, proof)?)
    }
}
