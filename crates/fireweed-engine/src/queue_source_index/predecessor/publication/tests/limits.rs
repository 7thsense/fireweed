use super::*;
#[test]
fn genesis_has_exact_six_facts_limits_scope_and_root_admission() {
    let h = Harness::new(mixed_schema());
    let checked = h.history[0].checked().unwrap();
    let cut = QueueSourceCut::from_captured(
        &h.reader.identity,
        h.reader.cut().unwrap().address(),
        1,
        0,
        &h.job,
    )
    .unwrap();
    let baseline = h.admission.usage().unwrap();
    let compiled = compile_genesis(
        &checked,
        cut,
        &h.definition,
        QueueSourceCompileLimits {
            edits: 6,
            input_bytes: 1 << 20,
        },
        &h.job,
    )
    .unwrap();
    let wire: u64 = compiled
        .edits()
        .map(|(key, value)| key.len() as u64 + value.unwrap().len() as u64)
        .sum();
    assert_eq!(compiled.edits().len(), 6);
    let identity = Arc::new(h.job.owned_buffer(128).unwrap());
    let foreign_cut =
        QueueSourceCut::from_captured(&identity, cut.address(), 1, 0, &h.job).unwrap();
    assert!(compiled.verify(&foreign_cut, &h.job).is_err());
    drop(identity);
    drop(compiled);
    assert_eq!(h.admission.usage().unwrap(), baseline);
    for limits in [
        QueueSourceCompileLimits {
            edits: 6,
            input_bytes: wire,
        },
        QueueSourceCompileLimits {
            edits: 5,
            input_bytes: wire,
        },
        QueueSourceCompileLimits {
            edits: 6,
            input_bytes: wire - 1,
        },
    ] {
        let result = compile_genesis(&checked, cut, &h.definition, limits, &h.job);
        assert_eq!(
            result.is_ok(),
            limits.edits == 6 && limits.input_bytes == wire
        );
        drop(result);
        assert_eq!(h.admission.usage().unwrap(), baseline);
    }
    let limits = QueueSourceCompileLimits {
        edits: 6,
        input_bytes: wire,
    };
    assert!(
        compile_genesis(
            &checked,
            h.reader.cut().unwrap(),
            &h.definition,
            limits,
            &h.job
        )
        .is_err()
    );
    let foreign = Arc::new(h.admission.reserve(0).unwrap());
    assert!(compile_genesis(&checked, cut, &h.definition, limits, &foreign).is_err());
    drop(foreign);
    assert_eq!(h.admission.usage().unwrap(), baseline);
    let available = (64 << 20) - baseline.1;
    let mut low = 1;
    let mut high = available;
    while low < high {
        let mid = low + (high - low) / 2;
        let blocker = h.job.owned_buffer(available - mid).unwrap();
        let result = compile_genesis(&checked, cut, &h.definition, limits, &h.job);
        let ok = result.is_ok();
        drop((result, blocker));
        assert_eq!(h.admission.usage().unwrap(), baseline);
        if ok {
            high = mid;
        } else {
            low = mid + 1;
        }
    }
    for allowance in [low - 1, low] {
        let blocker = h.job.owned_buffer(available - allowance).unwrap();
        let result = compile_genesis(&checked, cut, &h.definition, limits, &h.job);
        if allowance == low {
            result.as_ref().unwrap().verify(&cut, &h.job).unwrap();
        } else {
            assert!(
                matches!(result, Err(QueueError::OutcomeUnknown(ref message)) if message.contains("genesis output admission"))
            );
        }
        drop((result, blocker));
        assert_eq!(h.admission.usage().unwrap(), baseline);
    }
}

struct FaultReader<'a> {
    reader: &'a Reader,
    fail: Vec<u8>,
    hits: std::sync::atomic::AtomicUsize,
    pending: bool,
}
impl QueueSourceReader for FaultReader<'_> {
    type Point = Vec<u8>;
    type Range = (Vec<u8>, Vec<u8>);
    fn cut(&self) -> QueueResult<QueueSourceCut<'_>> {
        self.reader.cut()
    }
    async fn lookup(&self, key: &[u8]) -> QueueResult<Self::Point> {
        if key == self.fail {
            self.hits.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if self.pending {
                return std::future::pending().await;
            }
            return Err(QueueError::OutcomeUnknown(
                "exact injected late source failure".into(),
            ));
        }
        self.reader.lookup(key).await
    }
    fn point<'a>(
        &'a self,
        owner: &'a Self::Point,
    ) -> QueueResult<crate::queue_source_index::QueueSourcePoint<'a>> {
        self.reader.point(owner)
    }
    async fn complete_range(&self, lower: &[u8], upper: &[u8]) -> QueueResult<Self::Range> {
        self.reader.complete_range(lower, upper).await
    }
    fn visit_range(
        &self,
        owner: &Self::Range,
        lower: &[u8],
        upper: &[u8],
        visitor: &mut dyn for<'a> FnMut(
            crate::queue_source_index::QueueSourcePoint<'a>,
        ) -> QueueResult<()>,
    ) -> QueueResult<()> {
        self.reader.visit_range(owner, lower, upper, visitor)
    }
}

#[test]
fn complete_compiler_enforces_global_limits_and_required_late_source_checks() {
    futures::executor::block_on(async {
        let h = Harness::new(mixed_schema());
        let proposal = put_state("global-limit", "a\0", StateExpect::Absent, 1, -0.0);
        let (proposed, _) = record(&h.queue, &h.schema, &h.snapshot, &proposal, &h.job);
        let checked = proposed.checked().unwrap();
        let baseline = h.admission.usage().unwrap();
        let output = compile_publication(
            &h.reader,
            &checked,
            &h.definition,
            QueueSourceCompileLimits {
                edits: 1000,
                input_bytes: 1 << 20,
            },
            &h.job,
        )
        .await
        .unwrap();
        let count = output.edits().len() as u64;
        let bytes: u64 = output
            .edits()
            .map(|(key, value)| key.len() as u64 + value.map_or(0, |value| value.len() as u64))
            .sum();
        drop(output);
        assert_eq!(h.admission.usage().unwrap(), baseline);
        for limits in [
            QueueSourceCompileLimits {
                edits: count,
                input_bytes: bytes,
            },
            QueueSourceCompileLimits {
                edits: count - 1,
                input_bytes: bytes,
            },
            QueueSourceCompileLimits {
                edits: count,
                input_bytes: bytes - 1,
            },
        ] {
            let result =
                compile_publication(&h.reader, &checked, &h.definition, limits, &h.job).await;
            if limits.edits == count && limits.input_bytes == bytes {
                result
                    .as_ref()
                    .unwrap()
                    .verify(&h.reader.cut().unwrap(), &h.job)
                    .unwrap();
            } else {
                assert!(
                    matches!(result, Err(QueueError::OutcomeUnknown(ref message)) if message.contains("exceeds admitted limits"))
                );
            }
            drop(result);
            assert_eq!(h.admission.usage().unwrap(), baseline);
        }
        for empty in [false, true] {
            let proposal = if empty {
                group("empty-must-check-counts")
            } else {
                put_state(
                    "state-must-check-counts",
                    "a\0",
                    StateExpect::Absent,
                    1,
                    0.0,
                )
            };
            let (proposed, _) = record(&h.queue, &h.schema, &h.snapshot, &proposal, &h.job);
            let checked = proposed.checked().unwrap();
            let QueueLogPayload::Publication(publication) = checked.view().payload() else {
                panic!()
            };
            let receipt = publication.receipts().next().unwrap().unwrap();
            for fail in [
                QueueSourceKey::LifecycleCount(Lifecycle::Failed)
                    .encode()
                    .unwrap(),
                QueueSourceKey::Original {
                    domain: receipt.domain,
                    request_id: receipt.request_id,
                }
                .encode()
                .unwrap(),
            ] {
                let reader = FaultReader {
                    reader: &h.reader,
                    fail,
                    hits: std::sync::atomic::AtomicUsize::new(0),
                    pending: false,
                };
                let baseline = h.admission.usage().unwrap();
                let result = compile_publication(
                    &reader,
                    &checked,
                    &h.definition,
                    QueueSourceCompileLimits {
                        edits: 1000,
                        input_bytes: 1 << 20,
                    },
                    &h.job,
                )
                .await;
                assert!(
                    matches!(result, Err(QueueError::OutcomeUnknown(ref message)) if message == "exact injected late source failure")
                );
                assert_eq!(reader.hits.load(std::sync::atomic::Ordering::SeqCst), 1);
                drop(result);
                assert_eq!(h.admission.usage().unwrap(), baseline);
            }
        }
    });
}
#[test]
fn complete_dispatch_has_an_exact_retained_root_boundary() {
    futures::executor::block_on(async {
        let h = Harness::new(mixed_schema());
        let mut proposal = put_state("late-root", "state", StateExpect::Absent, 1, -0.0);
        proposal.work_inserts.push(NewWork {
            key: "work".into(),
            priority: -1,
            available_at: 0,
            fields: Fields::from([
                ("u".into(), FieldValue::Integer(2)),
                ("v".into(), FieldValue::Float(0.0)),
            ]),
            body: vec![1].into(),
        });
        let (proposed, _) = record(&h.queue, &h.schema, &h.snapshot, &proposal, &h.job);
        let checked = proposed.checked().unwrap();
        let limits = QueueSourceCompileLimits {
            edits: 10000,
            input_bytes: 1 << 20,
        };
        let baseline = h.admission.usage().unwrap();
        let available = (64 << 20) - baseline.1;
        let mut low = 1;
        let mut high = available;
        while low < high {
            let mid = low + (high - low) / 2;
            let blocker = h.job.owned_buffer(available - mid).unwrap();
            let result =
                compile_publication(&h.reader, &checked, &h.definition, limits, &h.job).await;
            let ok = result.is_ok();
            drop((result, blocker));
            assert_eq!(h.admission.usage().unwrap(), baseline);
            if ok {
                high = mid;
            } else {
                low = mid + 1;
            }
        }
        for allowance in [low - 1, low] {
            let blocker = h.job.owned_buffer(available - allowance).unwrap();
            let result =
                compile_publication(&h.reader, &checked, &h.definition, limits, &h.job).await;
            if allowance == low {
                result
                    .as_ref()
                    .unwrap()
                    .verify(&h.reader.cut().unwrap(), &h.job)
                    .unwrap();
            } else {
                assert!(
                    matches!(result, Err(QueueError::OutcomeUnknown(ref message)) if message.contains("source merge output admission"))
                );
            }
            drop((result, blocker));
            assert_eq!(h.admission.usage().unwrap(), baseline);
        }
    });
}

#[test]
fn dropping_compilation_during_pending_original_io_releases_every_partial_owner() {
    futures::executor::block_on(async {
        let h = Harness::new(mixed_schema());
        let proposal = put_state("pending-original", "state", StateExpect::Absent, 1, -0.0);
        let (proposed, _) = record(&h.queue, &h.schema, &h.snapshot, &proposal, &h.job);
        let checked = proposed.checked().unwrap();
        let QueueLogPayload::Publication(publication) = checked.view().payload() else {
            panic!()
        };
        let receipt = publication.receipts().next().unwrap().unwrap();
        let reader = FaultReader {
            reader: &h.reader,
            fail: QueueSourceKey::Original {
                domain: receipt.domain,
                request_id: receipt.request_id,
            }
            .encode()
            .unwrap(),
            hits: std::sync::atomic::AtomicUsize::new(0),
            pending: true,
        };
        let baseline = h.admission.usage().unwrap();
        let mut future = Box::pin(compile_publication(
            &reader,
            &checked,
            &h.definition,
            QueueSourceCompileLimits {
                edits: 1000,
                input_bytes: 1 << 20,
            },
            &h.job,
        ));
        assert!(matches!(
            futures::poll!(&mut future),
            std::task::Poll::Pending
        ));
        assert_eq!(reader.hits.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(h.admission.usage().unwrap().1 > baseline.1);
        drop(future);
        assert_eq!(h.admission.usage().unwrap(), baseline);
    });
}
