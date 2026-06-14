pub const DDL: &str = r#"
CREATE TABLE IF NOT EXISTS pqueue_queues (
  tenant_id                    text        NOT NULL,
  queue_id                     text        NOT NULL,
  priority_model               jsonb       NOT NULL,
  ordering_mode                text        NOT NULL,
  group_co_residency           boolean     NOT NULL DEFAULT false,
  recurring                    boolean     NOT NULL DEFAULT false,
  progress_bound_ms            bigint      NOT NULL,
  eligibility_policy           jsonb       NOT NULL DEFAULT '{}'::jsonb,
  request_id_retention_ms      bigint      NOT NULL,
  client_item_key_retention_ms bigint      NOT NULL,
  terminal_retention_ms        bigint      NOT NULL DEFAULT 0,
  max_lease_duration_ms        bigint      NOT NULL,
  retry_policy                 jsonb       NOT NULL,
  max_push_batch_size          bigint      NOT NULL,
  max_claim_batch_size         bigint      NOT NULL,
  max_eligible_group_size      bigint,
  cohort_policy                jsonb       NOT NULL DEFAULT '{"enabled":false}'::jsonb,
  recurrence_policy            jsonb       NOT NULL DEFAULT '{"mode":"oneshot","until":null}'::jsonb,
  backend_profile              text        NOT NULL DEFAULT 'postgres_native',
  shard_count                  integer     NOT NULL,
  created_at                   timestamptz NOT NULL DEFAULT now(),
  updated_at                   timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, queue_id)
);

CREATE TABLE IF NOT EXISTS pqueue_shards (
  tenant_id        text        NOT NULL,
  queue_id         text        NOT NULL,
  shard_id         integer     NOT NULL,
  assignment_epoch bigint      NOT NULL DEFAULT 1,
  placement        jsonb       NOT NULL DEFAULT '{}'::jsonb,
  state            text        NOT NULL DEFAULT 'unassigned',
  active_owner_id  text,
  target_owner_id  text,
  lease_expires_at timestamptz,
  updated_at       timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, queue_id, shard_id),
  FOREIGN KEY (tenant_id, queue_id) REFERENCES pqueue_queues (tenant_id, queue_id)
);
"#;
