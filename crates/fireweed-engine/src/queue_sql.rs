//! Shared bounded-depth native boolean SQL emission. Callers admit their
//! owned statement buffers before constructing expressions.
// Balanced SQL avoids turning a wide n-ary selector into a parser/planner's
// left-deep binary expression. Selector ownership and traversal remain flat.
pub fn balanced_boolean(mut expressions: Vec<String>, all: bool) -> String {
    if expressions.is_empty() {
        return if all { "TRUE" } else { "FALSE" }.into();
    }
    while expressions.len() > 1 {
        let mut next = Vec::with_capacity(expressions.len().div_ceil(2));
        let mut items = expressions.into_iter();
        while let Some(left) = items.next() {
            next.push(match items.next() {
                Some(right) => format!("({left} {} {right})", if all { "AND" } else { "OR" }),
                None => left,
            });
        }
        expressions = next;
    }
    expressions.pop().expect("one expression")
}

/// Compile a validated flat selector, delegating only storage-specific leaves.
/// Callers validate field types and admit statement buffers before this traversal.
/// N-ary groups are emitted with balanced SQL depth for wide legal selectors.
pub fn administrative_selector_sql(
    selector: &fireweed_core::queue_administration::WorkSelector,
    mut leaf: impl FnMut(
        &fireweed_core::queue_administration::SelectorNode,
    ) -> fireweed_core::queue_contract::QueueResult<String>,
) -> fireweed_core::queue_contract::QueueResult<String> {
    use fireweed_core::queue_administration::SelectorNode;
    use fireweed_core::queue_contract::QueueError;
    let failure = |message: &str| QueueError::OutcomeUnknown(message.into());
    let mut stack = Vec::new();
    for node in &selector.nodes {
        let sql = match node {
            SelectorNode::All(arity) | SelectorNode::Any(arity) => {
                let start = stack
                    .len()
                    .checked_sub(*arity as usize)
                    .ok_or_else(|| failure("invalid selector arity"))?;
                balanced_boolean(stack.split_off(start), matches!(node, SelectorNode::All(_)))
            }
            SelectorNode::Not => format!(
                "(NOT {})",
                stack
                    .pop()
                    .ok_or_else(|| failure("invalid selector negation"))?
            ),
            _ => leaf(node)?,
        };
        stack.push(sql);
    }
    if stack.len() != 1 {
        return Err(failure("invalid selector roots"));
    }
    Ok(stack.pop().expect("one root"))
}
