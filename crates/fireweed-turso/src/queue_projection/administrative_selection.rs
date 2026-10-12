//! Native leaf reads in one authoritative snapshot; native disk sets lower the
//! postfix Boolean program. No matched-key collection or client filtering.
use super::filter_sql::BytePool;
use super::*;
use crate::queue_scratch::NativeScratchDatabase;
use fireweed_core::queue_administration::{
    AdministrativeClause, Comparison, SelectorNode, WorkSelector,
};
use fireweed_engine::queue_index::scalar_type_prefix;
use fireweed_queue_spool::ScratchDiskQuota;

pub(super) struct Selection {
    scratch: NativeScratchDatabase,
    _plans: OwnedQueueBufferCharge,
}
struct Leaf {
    sql: String,
    bytes: Vec<u8>,
    _charge: OwnedQueueBufferCharge,
}
fn comparison(value: Comparison) -> &'static str {
    match value {
        Comparison::Equal => "=",
        Comparison::Before => "<",
        Comparison::BeforeOrEqual => "<=",
        Comparison::After => ">",
        Comparison::AfterOrEqual => ">=",
    }
}
fn base(queue: i64, predicate: &str, indexed: bool) -> String {
    format!(
        "SELECT r.id,length(r.key),r.version FROM q7_records r{} WHERE r.queue_id={queue} AND r.collection=0 AND r.active=1 AND ({predicate})",
        if indexed {
            " INDEXED BY q7_record_key"
        } else {
            ""
        }
    )
}
fn field_identity(
    name: &str,
    definition: &CollectionDefinition,
    pool: &mut BytePool,
) -> QueueResult<(&'static str, String)> {
    let field = field_number(definition, name)?;
    if field >= 0 {
        Ok(("q7_fields", format!("f.field={field}")))
    } else {
        let hash = pool.bytes(&discovery_digest(name.as_bytes()));
        let name = pool.bytes(name.as_bytes());
        Ok((
            "q7_additional",
            format!("f.name_hash={hash} AND f.name={name}"),
        ))
    }
}
/// One scalar probe, never a field-intersection aggregate or a wide IN program.
fn scalar_field(
    queue: i64,
    name: &str,
    predicate: &Predicate,
    definition: &CollectionDefinition,
    pool: &mut BytePool,
) -> QueueResult<String> {
    let (table, identity) = field_identity(name, definition, pool)?;
    let (predicate, index) = match predicate {
        Predicate::Equal(value) => {
            let encoded = ordered_key(&[Some(value)])?;
            let hash = pool.bytes(&discovery_digest(&encoded));
            let key = pool.bytes(&encoded);
            (
                format!("f.hash={hash} AND f.full_key={key}"),
                if table == "q7_fields" {
                    "q7_field_equal"
                } else {
                    "q7_additional_equal"
                },
            )
        }
        Predicate::Range { lower, upper } => {
            let bound = lower
                .as_ref()
                .or(upper.as_ref())
                .ok_or_else(|| unknown("empty administrative range"))?;
            let value = match bound {
                Bound::Included(v) | Bound::Excluded(v) => v,
            };
            let floor = scalar_type_prefix(value.field_type());
            let mut ceiling = floor;
            ceiling[2] += 1;
            let floor = pool.bytes(&floor);
            let ceiling = pool.bytes(&ceiling);
            let mut parts = vec![format!("f.prefix>={floor} AND f.prefix<{ceiling}")];
            for (bound, lower) in [(lower, true), (upper, false)] {
                if let Some(bound) = bound {
                    let (value, inclusive) = match bound {
                        Bound::Included(v) => (v, true),
                        Bound::Excluded(v) => (v, false),
                    };
                    let encoded = ordered_key(&[Some(value)])?;
                    let prefix = pool.bytes(&discovery_prefix(&encoded));
                    let key = pool.bytes(&encoded);
                    let op = match (lower, inclusive) {
                        (true, true) => ">=",
                        (true, false) => ">",
                        (false, true) => "<=",
                        (false, false) => "<",
                    };
                    parts.push(format!(
                        "f.prefix{}{prefix} AND f.full_key{op}{key}",
                        if lower { ">=" } else { "<=" }
                    ));
                }
            }
            (
                parts.join(" AND "),
                if table == "q7_fields" {
                    "q7_field_range"
                } else {
                    "q7_additional_range"
                },
            )
        }
        Predicate::In(_) => return Err(unknown("membership scalar was not expanded")),
    };
    Ok(format!(
        "SELECT r.id,length(r.key),r.version FROM {table} f INDEXED BY {index} JOIN q7_records r ON r.id=f.record_id WHERE f.queue_id={queue} AND f.collection=0 AND {identity} AND {predicate} AND r.queue_id={queue} AND r.collection=0 AND r.active=1"
    ))
}
fn leaf(
    queue: i64,
    node: &SelectorNode,
    definition: &CollectionDefinition,
    evaluated: i64,
    (scalar, key, state): (Option<&FieldValue>, Option<&str>, Option<Lifecycle>),
    job: &Arc<QueueReservation>,
) -> QueueResult<Leaf> {
    let charge = job.owned_buffer(
        encoded_len(node)?
            .checked_mul(16)
            .and_then(|n| n.checked_add(65536))
            .ok_or(QueueError::TooLarge)?,
    )?;
    let mut pool = BytePool(Vec::new());
    let active = format!("(r.lifecycle=1 AND COALESCE(r.lease_until>{evaluated},0))");
    let sql = match node {
        SelectorNode::Field { name, predicate } => match scalar {
            Some(value) => scalar_field(
                queue,
                name,
                &Predicate::Equal(value.clone()),
                definition,
                &mut pool,
            )?,
            None => scalar_field(queue, name, predicate, definition, &mut pool)?,
        },
        SelectorNode::Present { name, present } => {
            let (table, identity) = field_identity(name, definition, &mut pool)?;
            base(
                queue,
                &format!(
                    "{}EXISTS(SELECT 1 FROM {table} f WHERE f.record_id=r.id AND f.queue_id=r.queue_id AND f.collection=0 AND {identity})",
                    if *present { "" } else { "NOT " }
                ),
                false,
            )
        }
        SelectorNode::Keys(_) => {
            let key = key.ok_or_else(|| unknown("key scalar missing"))?;
            let hash = pool.bytes(&discovery_digest(key.as_bytes()));
            let key = pool.bytes(key.as_bytes());
            base(queue, &format!("r.key_hash={hash} AND r.key={key}"), true)
        }
        SelectorNode::StoredLifecycleIn(_) => base(
            queue,
            &format!(
                "r.lifecycle={}",
                encode_lifecycle(state.ok_or_else(|| unknown("lifecycle scalar missing"))?)
            ),
            false,
        ),
        SelectorNode::EffectiveLifecycleIn(_) => base(
            queue,
            &format!(
                "(CASE WHEN r.lifecycle=1 AND NOT {active} THEN 0 ELSE r.lifecycle END)={}",
                encode_lifecycle(state.ok_or_else(|| unknown("lifecycle scalar missing"))?)
            ),
            false,
        ),
        SelectorNode::Attempts {
            comparison: op,
            value,
        } => base(
            queue,
            &format!("r.attempts{}{value}", comparison(*op)),
            false,
        ),
        SelectorNode::AvailableAt {
            comparison: op,
            value,
        } => base(
            queue,
            &format!("r.available_at{}({value})", comparison(*op)),
            false,
        ),
        SelectorNode::LeaseActive(value) => base(
            queue,
            &format!("{}({active})", if *value { "" } else { "NOT " }),
            false,
        ),
        _ => return Err(unknown("boolean node reached leaf source")),
    };
    Ok(Leaf {
        sql,
        bytes: pool.0,
        _charge: charge,
    })
}

async fn qualify(connection: &Connection, plan: &Leaf) -> QueueResult<()> {
    let mut rows = connection
        .query(
            &format!("EXPLAIN {}", plan.sql),
            turso::params_from_iter(if plan.sql.contains("?1") {
                vec![blob(&plan.bytes)]
            } else {
                vec![]
            }),
        )
        .await
        .map_err(unknown)?;
    let mut count = 0;
    while let Some(row) = rows.next().await.map_err(unknown)? {
        let Value::Text(op) = row.get_value(1).map_err(unknown)? else {
            return Err(unknown("invalid selector plan"));
        };
        if op.starts_with("RowSet")
            || op.starts_with("Sorter")
            || op.starts_with("Hash")
            || matches!(
                op.as_str(),
                "OpenEphemeral" | "OpenAutoindex" | "Filter" | "FilterAdd"
            )
        {
            return Err(unknown(
                "selector plan materializes an unbounded collection",
            ));
        }
        count += 1;
    }
    if count == 0 {
        return Err(unknown("empty selector plan"));
    }
    Ok(())
}
async fn feed(
    connection: &Connection,
    queue: i64,
    plan: Leaf,
    node: i64,
    scratch: &mut NativeScratchDatabase,
    job: &Arc<QueueReservation>,
) -> QueueResult<()> {
    qualify(connection, &plan).await?;
    let _cursor = job.owned_buffer(65536)?;
    let mut rows = connection
        .query(
            &plan.sql,
            turso::params_from_iter(if plan.sql.contains("?1") {
                vec![blob(&plan.bytes)]
            } else {
                vec![]
            }),
        )
        .await
        .map_err(unknown)?;
    while let Some(row) = rows.next().await.map_err(unknown)? {
        let id = integer(&row.get_value(0).map_err(unknown)?)?;
        let length = integer(&row.get_value(1).map_err(unknown)?)?;
        let version = unsigned(&row.get_value(2).map_err(unknown)?)?;
        if id <= 0 || length <= 0 || version == 0 {
            return Err(unknown("invalid selected work metadata"));
        }
        let _key = job.owned_buffer(
            u64::try_from(length)
                .map_err(unknown)?
                .checked_mul(32)
                .and_then(|n| n.checked_add(65536))
                .ok_or(QueueError::TooLarge)?,
        )?;
        let key=one(connection,"SELECT key,version FROM q7_records WHERE id=?1 AND queue_id=?2 AND collection=0 AND active=1",vec![Value::Integer(id),Value::Integer(queue)],2).await?.ok_or_else(||unknown("selected work disappeared"))?;
        let key_bytes = bytes(&key[0])?;
        if key_bytes.len() as i64 != length || unsigned(&key[1])? != version {
            return Err(unknown("selected work metadata changed"));
        }
        std::str::from_utf8(key_bytes).map_err(unknown)?;
        scratch.record(id, key_bytes, version).await?;
        scratch.leaf(node, id).await?;
    }
    Ok(())
}
async fn metadata_guard(connection: &Connection, queue: i64, sequence: u64) -> QueueResult<()> {
    // Keep each native predicate shallow as well as independent of selector width.
    for predicate in [
        "typeof(r.active)<>'integer' OR r.active NOT IN (0,1)".to_owned(),
        format!("r.active=1 AND NOT ({})", super::work::raw_valid_authority()),
        "r.active=1 AND NOT (typeof(r.id)='integer' AND r.id>0 AND typeof(r.key)='blob' AND length(r.key)>0)".to_owned(),
        "r.active=1 AND NOT (typeof(r.key_hash)='blob' AND length(r.key_hash)=32 AND typeof(r.version)='blob' AND length(r.version)=8 AND r.version<>x'0000000000000000')".to_owned(),
        "r.active=1 AND NOT (typeof(r.priority)='integer' AND typeof(r.attempts)='integer' AND r.attempts>=0 AND r.attempts<=4294967295)".to_owned(),
        "r.active=1 AND NOT (typeof(r.insertion_sequence)='blob' AND length(r.insertion_sequence)=8 AND r.insertion_sequence>x'0000000000000000' AND r.insertion_sequence<=?1)".to_owned(),
    ] {
        let sql = format!("SELECT 1 FROM q7_records r WHERE r.queue_id={queue} AND r.collection=0 AND ({predicate}) LIMIT 1");
        let params = if sql.contains("?1") { vec![number(sequence)] } else { vec![] };
        if one(connection, &sql, params, 1).await?.is_some() {
            return Err(unknown("invalid administrative work authority"));
        }
    }

    Ok(())
}
#[allow(clippy::too_many_arguments)]
async fn lower(
    connection: &Connection,
    queue: i64,
    definition: &CollectionDefinition,
    selector: &WorkSelector,
    evaluated: i64,
    scratch: &mut NativeScratchDatabase,
    next: &mut i64,
    universe: &mut bool,
    job: &Arc<QueueReservation>,
) -> QueueResult<i64> {
    selector.validate(definition)?;
    let mut stack = Vec::<i64>::with_capacity(selector.nodes.len());
    for item in &selector.nodes {
        let node = *next;
        *next = next
            .checked_add(1)
            .ok_or_else(|| unknown("selector node identity exhausted"))?;
        let arity = match item {
            SelectorNode::All(n) | SelectorNode::Any(n) => *n as usize,
            SelectorNode::Not => 1,
            _ => 0,
        };
        let start = stack
            .len()
            .checked_sub(arity)
            .ok_or_else(|| unknown("selector operand underflow"))?;
        match item {
            SelectorNode::All(_) | SelectorNode::Any(_) | SelectorNode::Not => {
                let children = stack.split_off(start);
                let needs_universe = matches!(item, SelectorNode::Not | SelectorNode::All(0));
                if needs_universe && !*universe {
                    let plan = Leaf {
                        sql: base(queue, "1", false),
                        bytes: vec![],
                        _charge: job.owned_buffer(65536)?,
                    };
                    feed(connection, queue, plan, 0, scratch, job).await?;
                    *universe = true;
                }
                match item {
                    SelectorNode::Any(_) => {
                        for child in children {
                            let mut after = 0;
                            while let Some(id) = scratch.next_set(child, after).await? {
                                scratch.union_child(node, id, child).await?;
                                after = id;
                            }
                        }
                    }
                    SelectorNode::All(_) => {
                        let basis = children.first().copied().unwrap_or(0);
                        for child in children {
                            scratch.edge(node, child).await?;
                        }
                        let mut after = 0;
                        while let Some(id) = scratch.next_set(basis, after).await? {
                            scratch.intersection(node, id).await?;
                            after = id;
                        }
                    }
                    SelectorNode::Not => {
                        let child = children[0];
                        let mut after = 0;
                        while let Some(id) = scratch.next_set(0, after).await? {
                            scratch.complement(node, id, child).await?;
                            after = id;
                        }
                    }
                    _ => unreachable!(),
                }
                scratch.finish_node(node).await?;
            }
            SelectorNode::Keys(keys) => {
                for key in keys {
                    feed(
                        connection,
                        queue,
                        leaf(
                            queue,
                            item,
                            definition,
                            evaluated,
                            (None, Some(key), None),
                            job,
                        )?,
                        node,
                        scratch,
                        job,
                    )
                    .await?;
                }
            }
            SelectorNode::Field {
                predicate: Predicate::In(values),
                ..
            } => {
                for value in values {
                    feed(
                        connection,
                        queue,
                        leaf(
                            queue,
                            item,
                            definition,
                            evaluated,
                            (Some(value), None, None),
                            job,
                        )?,
                        node,
                        scratch,
                        job,
                    )
                    .await?;
                }
            }
            SelectorNode::StoredLifecycleIn(states)
            | SelectorNode::EffectiveLifecycleIn(states) => {
                for state in states {
                    feed(
                        connection,
                        queue,
                        leaf(
                            queue,
                            item,
                            definition,
                            evaluated,
                            (None, None, Some(*state)),
                            job,
                        )?,
                        node,
                        scratch,
                        job,
                    )
                    .await?;
                }
            }
            _ => {
                feed(
                    connection,
                    queue,
                    leaf(queue, item, definition, evaluated, (None, None, None), job)?,
                    node,
                    scratch,
                    job,
                )
                .await?
            }
        }
        stack.push(node);
    }
    if stack.len() != 1 {
        return Err(unknown("selector has disconnected roots"));
    }
    Ok(stack[0])
}
impl Selection {
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn prepare(
        connection: &Connection,
        queue: i64,
        schema: &QueueSchema,
        sequence: u64,
        clauses: &[AdministrativeClause],
        evaluated: i64,
        root: Arc<fireweed_queue_spool::ScratchRoot>,
        quota: Arc<ScratchDiskQuota>,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<Self> {
        let nodes = clauses.iter().try_fold(0u64, |n, c| {
            n.checked_add(c.selector.nodes.len() as u64)
                .ok_or(QueueError::TooLarge)
        })?;
        let plans = job.owned_buffer(
            nodes
                .checked_mul(128)
                .and_then(|n| n.checked_add(65536))
                .ok_or(QueueError::TooLarge)?,
        )?;
        metadata_guard(connection, queue, sequence).await?;
        let mut scratch = NativeScratchDatabase::open(root, quota, job).await?;
        let mut next = 1;
        let mut universe = false;
        for (ordinal, clause) in clauses.iter().enumerate() {
            let node = Box::pin(lower(
                connection,
                queue,
                &schema.work,
                &clause.selector,
                evaluated,
                &mut scratch,
                &mut next,
                &mut universe,
                job,
            ))
            .await?;
            let mut after = 0;
            while let Some(id) = scratch.next_set(node, after).await? {
                scratch
                    .owner(id, i64::try_from(ordinal).map_err(unknown)?)
                    .await?;
                after = id;
            }
        }
        Ok(Self {
            scratch,
            _plans: plans,
        })
    }
    pub(super) async fn next(
        &mut self,
        after: &[u8],
    ) -> QueueResult<Option<crate::queue_scratch::SelectedScratchRow>> {
        self.scratch.next_owner(after).await
    }
}
