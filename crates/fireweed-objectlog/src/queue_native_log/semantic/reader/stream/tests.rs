use super::*;

pub(in crate::queue_native_log::semantic::reader) async fn contract(
    source: &QueueNativeCaptured,
    log: &QueueNativeLog,
    job: &Arc<QueueReservation>,
    other: &QueueNativeCaptured,
    admission: &Arc<fireweed_engine::queue_admission::QueueAdmission>,
) {
    let work = QueueNativeSemanticWork {
        pages: 1000,
        hydrated_bytes: 1 << 16,
    };
    let limits = QueueNativeSourceReadLimits {
        page: QueueNativeScanLimits {
            rows: 1,
            output_bytes: 8192,
            work,
        },
        range_pages: 1,
    };
    let budget = |calls| {
        QueueNativeSourceBudget::new(
            QueueNativeSourceWork {
                semantic: QueueNativeSemanticWork {
                    pages: work.pages * calls,
                    hydrated_bytes: work.hydrated_bytes * calls,
                },
                record_attempts: 0,
                hydration_reads: 0,
                hydration_body_bytes: 0,
            },
            job,
        )
        .unwrap()
    };
    let ledger = budget(100);
    let reader = QueueNativeSourceReader::new(source, log, job, ledger.clone(), limits).unwrap();
    let other_reader =
        QueueNativeSourceReader::new(other, log, job, ledger.clone(), limits).unwrap();
    let baseline = admission.usage().unwrap();
    {
        let _copies = job.owned_buffer(65536).unwrap();
        let mut cursor = reader.open_stream(b"a", b"z").unwrap();
        let mut keys = Vec::new();
        let mut pages = 0;
        while let Some(page) = reader.next_stream_page(&mut cursor).await.unwrap() {
            pages += 1;
            if pages == 4 {
                assert_eq!(page.page.status(), QueueNativeScanStatus::Complete);
                assert!(page.page.inner.rows().is_empty());
            }
            reader
                .visit_stream_page(&mut cursor, page, &mut |point| {
                    assert!(point.value_for(&reader.cut()?, point.key())?.is_some());
                    keys.push(point.key().to_vec());
                    Ok(())
                })
                .unwrap();
        }
        assert_eq!(
            pages, 4,
            "streaming must cross retained-range page cap1 and visit empty terminal page"
        );
        assert_eq!(keys, [b"a".to_vec(), b"b\0full".to_vec(), b"c".to_vec()]);
        let completion = reader.finish_stream(cursor).unwrap();
        reader
            .validate_stream_completion(&completion, b"a", b"z")
            .unwrap();
        assert!(
            reader
                .validate_stream_completion(&completion, b"a", b"c")
                .is_err()
        );
        assert!(
            other_reader
                .validate_stream_completion(&completion, b"a", b"z")
                .is_err()
        );
    }
    assert_eq!(admission.usage().unwrap(), baseline);
    assert!(reader.open_stream(b"z", b"a").is_err());
    assert!(
        reader
            .finish_stream(reader.open_stream(b"a", b"z").unwrap())
            .is_err()
    );
    {
        let mut cursor = reader.open_stream(b"a", b"z").unwrap();
        let page = reader.next_stream_page(&mut cursor).await.unwrap().unwrap();
        drop(page);
        assert!(reader.next_stream_page(&mut cursor).await.is_err());
        assert!(reader.finish_stream(cursor).is_err());
    }
    {
        let mut cursor = reader.open_stream(b"a", b"z").unwrap();
        for _ in 0..3 {
            let page = reader.next_stream_page(&mut cursor).await.unwrap().unwrap();
            reader
                .visit_stream_page(&mut cursor, page, &mut |_| Ok(()))
                .unwrap();
        }
        let terminal = reader.next_stream_page(&mut cursor).await.unwrap().unwrap();
        assert_eq!(terminal.page.status(), QueueNativeScanStatus::Complete);
        drop(terminal);
        assert!(
            reader.finish_stream(cursor).is_err(),
            "unvisited empty terminal page cannot seal prefix"
        );
    }
    {
        let mut a = reader.open_stream(b"a", b"z").unwrap();
        let mut b = reader.open_stream(b"b", b"z").unwrap();
        let first = reader.next_stream_page(&mut a).await.unwrap().unwrap();
        let second = reader.next_stream_page(&mut b).await.unwrap().unwrap();
        let mut visits = 0;
        assert!(
            reader
                .visit_stream_page(&mut b, first, &mut |_| {
                    visits += 1;
                    Ok(())
                })
                .is_err()
        );
        assert_eq!(visits, 0);
        drop(second);
        assert!(reader.finish_stream(a).is_err());
        assert!(reader.finish_stream(b).is_err());
    }
    {
        let mut cursor = reader.open_stream(b"a", b"z").unwrap();
        let page = reader.next_stream_page(&mut cursor).await.unwrap().unwrap();
        let mut visits = 0;
        assert!(
            other_reader
                .visit_stream_page(&mut cursor, page, &mut |_| {
                    visits += 1;
                    Ok(())
                })
                .is_err()
        );
        assert_eq!(visits, 0);
        assert!(reader.finish_stream(cursor).is_err());
    }
    for unwind in [false, true] {
        let mut cursor = reader.open_stream(b"a", b"z").unwrap();
        let page = reader.next_stream_page(&mut cursor).await.unwrap().unwrap();
        if unwind {
            assert!(
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let _ = reader.visit_stream_page(&mut cursor, page, &mut |_| {
                        panic!("injected stream visitor unwind")
                    });
                }))
                .is_err()
            );
        } else {
            assert!(
                reader
                    .visit_stream_page(&mut cursor, page, &mut |_| Err(unknown(
                        "injected stream visitor failure"
                    )))
                    .is_err()
            );
        }
        assert!(reader.next_stream_page(&mut cursor).await.is_err());
        assert!(reader.finish_stream(cursor).is_err());
    }
    {
        let mut cursor = reader.open_stream(b"a", b"z").unwrap();
        let first = reader.next_stream_page(&mut cursor).await.unwrap().unwrap();
        reader
            .visit_stream_page(&mut cursor, first, &mut |_| Ok(()))
            .unwrap();
        let mut second = reader.next_stream_page(&mut cursor).await.unwrap().unwrap();
        second.ordinal -= 1;
        let mut visits = 0;
        assert!(
            reader
                .visit_stream_page(&mut cursor, second, &mut |_| {
                    visits += 1;
                    Ok(())
                })
                .is_err()
        );
        assert_eq!(visits, 0);
        assert!(reader.finish_stream(cursor).is_err());
    }
    {
        let replacement =
            QueueNativeSourceReader::new(source, log, job, budget(8), limits).unwrap();
        let mut cursor = reader.open_stream(b"a", b"z").unwrap();
        assert!(
            replacement.next_stream_page(&mut cursor).await.is_err(),
            "same cut/job cannot replace cumulative ledger"
        );
        assert!(reader.finish_stream(cursor).is_err());
    }
    {
        let ledger = budget(3);
        let bounded =
            QueueNativeSourceReader::new(source, log, job, ledger.clone(), limits).unwrap();
        let mut cursor = bounded.open_stream(b"a", b"z").unwrap();
        for _ in 0..3 {
            let page = bounded
                .next_stream_page(&mut cursor)
                .await
                .unwrap()
                .unwrap();
            bounded
                .visit_stream_page(&mut cursor, page, &mut |_| Ok(()))
                .unwrap();
        }
        assert!(bounded.next_stream_page(&mut cursor).await.is_err());
        assert!(bounded.finish_stream(cursor).is_err());
        assert_eq!(ledger.remaining_work().semantic.pages, 0);
        assert!(
            bounded.lookup(b"a").await.is_err(),
            "no fresh credit after missing terminal grant"
        );
    }
    {
        let mut cursor = reader.open_stream(b"a", b"z").unwrap();
        let page = reader.next_stream_page(&mut cursor).await.unwrap().unwrap();
        reader
            .visit_stream_page(&mut cursor, page, &mut |_| Ok(()))
            .unwrap();
        let before = ledger.remaining_work();
        let (entered, observed) = tokio::sync::oneshot::channel();
        let (release, released) = tokio::sync::oneshot::channel();
        cursor.after_read = Some((entered, released));
        {
            let future = reader.next_stream_page(&mut cursor);
            tokio::pin!(future);
            tokio::select! {
                result = &mut future => panic!("later-page cancellation gate bypassed: {}", result.is_ok()),
                result = observed => result.unwrap(),
            }
        }
        drop(release);
        assert_eq!(
            ledger.remaining_work().semantic.pages,
            before.semantic.pages - work.pages
        );
        assert!(reader.next_stream_page(&mut cursor).await.is_err());
        assert!(
            reader.finish_stream(cursor).is_err(),
            "cancellation after later SDK hydration cannot certify EOF"
        );
    }

    {
        let mut cursor = reader.open_stream(b"a", b"z").unwrap();
        let changed = QueueNativeSourceReader::new(
            source,
            log,
            job,
            ledger.clone(),
            QueueNativeSourceReadLimits {
                page: QueueNativeScanLimits {
                    rows: 2,
                    ..limits.page
                },
                ..limits
            },
        )
        .unwrap();
        let before = ledger.remaining_work().semantic.pages;
        assert!(changed.next_stream_page(&mut cursor).await.is_err());
        assert_eq!(ledger.remaining_work().semantic.pages, before);
        assert!(reader.finish_stream(cursor).is_err());
    }
    {
        let tiny = QueueNativeSemanticWork {
            pages: 1,
            hydrated_bytes: 1,
        };
        let ledger = QueueNativeSourceBudget::new(
            QueueNativeSourceWork {
                semantic: tiny,
                record_attempts: 0,
                hydration_reads: 0,
                hydration_body_bytes: 0,
            },
            job,
        )
        .unwrap();
        let broken = QueueNativeSourceReader::new(
            source,
            log,
            job,
            ledger.clone(),
            QueueNativeSourceReadLimits {
                page: QueueNativeScanLimits {
                    work: tiny,
                    ..limits.page
                },
                ..limits
            },
        )
        .unwrap();
        let mut cursor = broken.open_stream(b"a", b"z").unwrap();
        assert!(
            broken.next_stream_page(&mut cursor).await.is_err(),
            "actual SDK work failure must poison before returning"
        );
        assert_eq!(ledger.remaining_work().semantic.pages, 0);
        assert!(broken.next_stream_page(&mut cursor).await.is_err());
        assert!(broken.finish_stream(cursor).is_err());
    }
    {
        let records_budget = budget(1);
        let records =
            QueueNativeSourceReader::new(source, log, job, records_budget.clone(), limits)
                .unwrap()
                .with_records(QueueNativeSourceRecordLimits {
                    payload_bytes: 1,
                    logical_identity_bytes: 1,
                    physical_identity_bytes: 1,
                    frame_bytes: 1,
                })
                .unwrap();
        let mut cursor = records.open_stream(b"x", b"z").unwrap();
        let page = records
            .next_stream_page(&mut cursor)
            .await
            .unwrap()
            .unwrap();
        let mut visits = 0;
        records
            .visit_stream_page(&mut cursor, page, &mut |_| {
                visits += 1;
                Ok(())
            })
            .unwrap();
        assert_eq!(visits, 0);
        assert!(
            records
                .next_stream_page(&mut cursor)
                .await
                .unwrap()
                .is_none()
        );
        let completion = records.finish_stream(cursor).unwrap();
        records
            .validate_stream_completion(&completion, b"x", b"z")
            .unwrap();
        assert_eq!(
            records_budget.remaining_work().record_attempts,
            0,
            "metadata stream never enables body hydration"
        );
    }

    {
        let exact = (std::mem::size_of::<QueueNativeSourceCursor>()
            + 2
            + std::mem::size_of::<OwnedQueueBufferCharge>()
            + 2 * std::mem::size_of::<usize>()) as u64;
        for spare in [exact - 1, exact] {
            let before = admission.usage().unwrap();
            let remaining = ledger.remaining_work().semantic.pages;
            let padding = job.owned_buffer((32 << 20) - before.1 - spare).unwrap();
            let padded = admission.usage().unwrap();
            let cursor = reader.open_stream(b"a", b"z");
            assert_eq!(
                cursor.is_ok(),
                spare == exact,
                "exact cursor container includes shared guard payload and Arc header"
            );
            if let Ok(cursor) = cursor {
                drop(cursor);
            }
            assert_eq!(admission.usage().unwrap(), padded);
            assert_eq!(ledger.remaining_work().semantic.pages, remaining);
            drop(padding);
            assert_eq!(admission.usage().unwrap(), before);
        }
        let mut cursor = reader.open_stream(b"a", b"z").unwrap();
        let page = reader.next_stream_page(&mut cursor).await.unwrap().unwrap();
        drop(cursor);
        let alias = page.cursor.clone();
        drop(page);
        assert_eq!(
            admission.usage().unwrap().1,
            baseline.1 + exact,
            "dropped cursor credit follows actual page's final shared owner"
        );
        drop(alias);
        assert_eq!(admission.usage().unwrap(), baseline);
    }
    {
        let wider = QueueNativeSourceReader::new(
            source,
            log,
            job,
            ledger.clone(),
            QueueNativeSourceReadLimits {
                page: QueueNativeScanLimits {
                    rows: 8,
                    ..limits.page
                },
                ..limits
            },
        )
        .unwrap();
        let mut cursor = wider.open_stream(b"a", b"z").unwrap();
        let page = wider.next_stream_page(&mut cursor).await.unwrap().unwrap();
        assert_eq!(page.page.inner.rows().len(), 3);
        // Test-only internal corruption puts the invalid row after a valid row;
        // prevalidation must reject before even that first callback is invoked.
        cursor.upper[0] = b'b';
        let mut visits = 0;
        assert!(
            wider
                .visit_stream_page(&mut cursor, page, &mut |_| {
                    visits += 1;
                    Ok(())
                })
                .is_err()
        );
        assert_eq!(visits, 0);
        assert!(wider.finish_stream(cursor).is_err());
    }
    assert_eq!(admission.usage().unwrap(), baseline);
}
