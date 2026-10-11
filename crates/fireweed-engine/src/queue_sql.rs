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
