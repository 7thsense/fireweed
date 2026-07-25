use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::performance_matrix::{OperationSamples, RepetitionResult};
use crate::performance_matrix_evidence::Summary;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Comparison {
    pub left: String,
    pub right: String,
    pub shape: String,
    pub operation: String,
    pub status: String,
    pub median_throughput_ratio: Option<f64>,
    pub rounds_left_faster: usize,
    pub rounds_right_faster: usize,
}

fn barrier_class(cell: &str) -> &'static str {
    match cell {
        "memory" => "volatile-visible",
        "sqlite-log" | "sqlite-relational" => "local-durable-visible",
        "postgres-log" | "postgres-relational" => "postgres-durable-visible",
        "objectlog-local-direct" => "objectlog-durable-visible",
        value if value.ends_with("sqlite-async") => "objectlog-hot-visible",
        _ => "objectlog-projection-visible",
    }
}

fn operation<'a>(row: &'a RepetitionResult, name: &str) -> &'a OperationSamples {
    match name {
        "append" => &row.append,
        "claim" => &row.claim,
        "finalize" => &row.finalize,
        _ => unreachable!("fixed operation list"),
    }
}

fn throughput(operation: &OperationSamples) -> f64 {
    operation.items as f64 / (operation.total_ns as f64 / 1_000_000_000.0)
}

pub fn build_comparisons(rows: &[RepetitionResult], summaries: &[Summary]) -> Vec<Comparison> {
    let summary_cv = summaries
        .iter()
        .map(|summary| {
            (
                (
                    summary.cell.as_str(),
                    summary.shape.as_str(),
                    summary.operation.as_str(),
                ),
                summary.throughput_cv,
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut cells = rows.iter().map(|row| row.cell.as_str()).collect::<Vec<_>>();
    cells.sort_unstable();
    cells.dedup();
    let mut shapes = rows
        .iter()
        .map(|row| row.shape.as_str())
        .collect::<Vec<_>>();
    shapes.sort_unstable();
    shapes.dedup();
    let mut output = Vec::new();
    for (left_index, left) in cells.iter().enumerate() {
        for right in cells.iter().skip(left_index + 1) {
            for shape in &shapes {
                for op in ["append", "claim", "finalize"] {
                    if barrier_class(left) != barrier_class(right) {
                        output.push(Comparison {
                            left: (*left).into(),
                            right: (*right).into(),
                            shape: (*shape).into(),
                            operation: op.into(),
                            status: "different_success_boundary".into(),
                            median_throughput_ratio: None,
                            rounds_left_faster: 0,
                            rounds_right_faster: 0,
                        });
                        continue;
                    }
                    let mut ratios = rows
                        .iter()
                        .filter(|row| row.cell == *left && row.shape == *shape)
                        .filter_map(|left_row| {
                            rows.iter()
                                .find(|right_row| {
                                    right_row.cell == *right
                                        && right_row.shape == *shape
                                        && right_row.repetition == left_row.repetition
                                })
                                .map(|right_row| {
                                    throughput(operation(left_row, op))
                                        / throughput(operation(right_row, op))
                                })
                        })
                        .collect::<Vec<_>>();
                    ratios.sort_by(f64::total_cmp);
                    let left_faster = ratios.iter().filter(|ratio| **ratio > 1.0).count();
                    let right_faster = ratios.iter().filter(|ratio| **ratio < 1.0).count();
                    let median = ratios.get(ratios.len() / 2).copied();
                    let stable = summary_cv
                        .get(&(*left, *shape, op))
                        .is_some_and(|cv| *cv <= 0.15)
                        && summary_cv
                            .get(&(*right, *shape, op))
                            .is_some_and(|cv| *cv <= 0.15);
                    let directional = (left_faster >= 4
                        && median.is_some_and(|ratio| ratio >= 1.10))
                        || (right_faster >= 4 && median.is_some_and(|ratio| ratio <= 1.0 / 1.10));
                    output.push(Comparison {
                        left: (*left).into(),
                        right: (*right).into(),
                        shape: (*shape).into(),
                        operation: op.into(),
                        status: if ratios.len() == 5 && stable && directional {
                            "material"
                        } else {
                            "inconclusive"
                        }
                        .into(),
                        median_throughput_ratio: median,
                        rounds_left_faster: left_faster,
                        rounds_right_faster: right_faster,
                    });
                }
            }
        }
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn barrier_classes_prevent_invalid_ratios() {
        assert_ne!(barrier_class("memory"), barrier_class("sqlite-log"));
        assert_eq!(
            barrier_class("sqlite-log"),
            barrier_class("sqlite-relational")
        );
    }
}
