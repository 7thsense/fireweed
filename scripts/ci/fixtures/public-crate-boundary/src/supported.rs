use std::{path::PathBuf, sync::Arc};

use fireweed::{
    ConfigSecret, ControlPlaneConfig, Fireweed, ObjectLogRuntimeConfig, ObjectLogStorage, OwnerId,
    PostgresCoordinationConfig, PostgresMode, PostgresRuntimeConfig, ProjectionConfig,
    RecoveryPolicy, ResponseBarrier, SegmentConfig,
};

#[allow(unused_imports)]
use fireweed::{
    ActiveScopeDiscovery as NamedActiveScopeDiscovery,
    ActiveScopeSelection as NamedActiveScopeSelection,
    AggregateGroup as NamedAggregateGroup, BucketRule as NamedBucketRule, Bytes as NamedBytes,
    ClaimAt as NamedClaimAt, ClaimByQueryAt as NamedClaimByQueryAt,
    ClaimByQueryRequest as NamedClaimByQueryRequest, ClaimCompatibility as NamedClaimCompatibility,
    ClaimRef as NamedClaimRef, Claimed as NamedClaimed, ClaimedItem as NamedClaimedItem,
    ClientItemKey as NamedClientItemKey, Clock as NamedClock,
    CommandPosition as NamedCommandPosition, CommitCapabilities as NamedCommitCapabilities,
    CommitEntry as NamedCommitEntry, CommitRecovery as NamedCommitRecovery,
    CommitRequest as NamedCommitRequest, CompoundIndexDef as NamedCompoundIndexDef,
    CompoundIndexField as NamedCompoundIndexField, CreateQueueOutcome as NamedCreateQueueOutcome,
    DeclaredBucketSegmentRequest as NamedDeclaredBucketSegmentRequest,
    DeclaredBucketSegmentResponse as NamedDeclaredBucketSegmentResponse,
    EligibilityPolicy as NamedEligibilityPolicy, EngineError as NamedEngineError,
    EngineResult as NamedEngineResult, EntryOutcome as NamedEntryOutcome, FilterOp as NamedFilterOp,
    FinalizeKind as NamedFinalizeKind, GroupByField as NamedGroupByField,
    GroupedAggregateRequest as NamedGroupedAggregateRequest,
    GroupedAggregateResponse as NamedGroupedAggregateResponse,
    IndexDeclaration as NamedIndexDeclaration, IndexHit as NamedIndexHit, IndexType as NamedIndexType,
    InstanceFence as NamedInstanceFence, ItemId as NamedItemId, LeaseToken as NamedLeaseToken,
    LiveItemView as NamedLiveItemView, Metadata as NamedMetadata, MetadataValue as NamedMetadataValue,
    MetricsByQueryRequest as NamedMetricsByQueryRequest, Nack as NamedNack, NewItem as NamedNewItem,
    OldestFirstScopePrefix as NamedOldestFirstScopePrefix, OrderField as NamedOrderField,
    OrderingMode as NamedOrderingMode, PriorityModel as NamedPriorityModel,
    PriorityValue as NamedPriorityValue, QueryCapabilityFlags as NamedQueryCapabilityFlags,
    QueryCursor as NamedQueryCursor, QueryFilter as NamedQueryFilter,
    ProjectionControl as NamedProjectionControl,
    ProjectionControlCapabilities as NamedProjectionControlCapabilities,
    ProjectionRebuild as NamedProjectionRebuild,
    ProjectionVerification as NamedProjectionVerification, RecoveryAction as NamedRecoveryAction,
    QueueDefinition as NamedQueueDefinition, QueueId as NamedQueueId, QueueIndex as NamedQueueIndex,
    QueueKey as NamedQueueKey, QueueMetrics as NamedQueueMetrics,
    RangeScanRequest as NamedRangeScanRequest, RangeScanResponse as NamedRangeScanResponse,
    RecurrencePolicy as NamedRecurrencePolicy, RequestId as NamedRequestId,
    RetryPolicy as NamedRetryPolicy, ScheduleUpdate as NamedScheduleUpdate, SideRecord as NamedSideRecord,
    SortDirection as NamedSortDirection, TenantId as NamedTenantId, TimeBucket as NamedTimeBucket,
    TypedValue as NamedTypedValue, UpsertOutcome as NamedUpsertOutcome,
    UtcTimestamp as NamedUtcTimestamp, WorkerId as NamedWorkerId,
    select_active_scope_from_prefix as named_select_active_scope_from_prefix,
};

fn assert_send_sync<T: Send + Sync>() {}

#[allow(dead_code)]
async fn borrowed_projection_control_survives_await(fireweed: Arc<Fireweed>) {
    if let Some(control) = fireweed.projection_control() {
        let _: fireweed::ProjectionControlCapabilities = control.capabilities();
        let _ = control.verify().await;
        let _ = control.delete().await;
        let _ = control.rebuild().await;
    }
}

fn objectlog_config(projection: ProjectionConfig) -> ObjectLogRuntimeConfig {
    ObjectLogRuntimeConfig {
        object_log: ObjectLogStorage::Local {
            root: PathBuf::from("object-log"),
        },
        projection,
        response_barrier: ResponseBarrier::Strict,
        segments: SegmentConfig::new(1024, 5).unwrap(),
        namespace: "fixture".to_owned(),
        recovery: RecoveryPolicy::default(),
    }
}

#[allow(dead_code)]
async fn every_constructor_returns_one_opaque_type() -> fireweed::EngineResult<()> {
    let clock = || Arc::new(fireweed::SystemClock) as Arc<dyn fireweed::Clock>;
    let _: Fireweed = fireweed::open_memory(clock());
    let _: Fireweed = fireweed::open_sqlite(":memory:", clock())?;
    let _: Fireweed = fireweed::open_sqlite_relational(":memory:", clock())?;
    let _: Fireweed = fireweed::open_objectlog(PathBuf::from("object-log"), clock())?;
    let _: Fireweed = fireweed::open_postgres("postgres://example", clock())?;
    let _: Fireweed = fireweed::open_postgres_async("postgres://example", clock()).await?;
    let _: Fireweed = fireweed::open_postgres_coordinated(
        "postgres://example",
        clock(),
        OwnerId::new("fixture").unwrap(),
        ControlPlaneConfig::default(),
    )?;
    let _: Fireweed = fireweed::open_postgres_runtime(
        PostgresRuntimeConfig {
            url: ConfigSecret::new("postgres://example"),
            schema: Some("fixture".to_owned()),
            mode: PostgresMode::Relational,
            node_id: Some(1),
            coordination: Some(PostgresCoordinationConfig {
                instance_id: OwnerId::new("fixture").unwrap(),
                control_plane: ControlPlaneConfig::default(),
            }),
        },
        clock(),
    )?;
    let _: Fireweed = fireweed::open_postgres_runtime_async(
        PostgresRuntimeConfig {
            url: ConfigSecret::new("postgres://example"),
            schema: Some("fixture_async".to_owned()),
            mode: PostgresMode::Relational,
            node_id: Some(1),
            coordination: None,
        },
        clock(),
    )
    .await?;
    let _: Fireweed = fireweed::open_objectlog_sqlite(
        objectlog_config(ProjectionConfig::Sqlite {
            path: PathBuf::from("projection.sqlite"),
        }),
        clock(),
    )?;
    let postgres_config = || {
        objectlog_config(ProjectionConfig::Postgres {
            url: ConfigSecret::new("postgres://example"),
        })
    };
    let _: Fireweed = fireweed::open_objectlog_postgres(postgres_config(), clock())?;
    let _: Fireweed = fireweed::open_objectlog_postgres_async(postgres_config(), clock()).await?;
    Ok(())
}

fn main() {
    assert_send_sync::<Fireweed>();
    let queue = fireweed::open_memory(Arc::new(fireweed::SystemClock));
    assert_eq!(format!("{queue:?}"), "Fireweed { .. }");
}
