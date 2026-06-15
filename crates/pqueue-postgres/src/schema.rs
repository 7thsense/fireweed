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
  tenant_id              text        NOT NULL,
  queue_id               text        NOT NULL,
  shard_id               integer     NOT NULL,
  assignment_epoch       bigint      NOT NULL DEFAULT 1,
  next_command_sequence  bigint      NOT NULL DEFAULT 0,
  placement              jsonb       NOT NULL DEFAULT '{}'::jsonb,
  state                  text        NOT NULL DEFAULT 'unassigned',
  active_owner_id        text,
  target_owner_id        text,
  lease_expires_at       timestamptz,
  updated_at             timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, queue_id, shard_id),
  FOREIGN KEY (tenant_id, queue_id) REFERENCES pqueue_queues (tenant_id, queue_id)
);

CREATE TABLE IF NOT EXISTS pqueue_commands (
  tenant_id           text        NOT NULL,
  queue_id            text        NOT NULL,
  shard_id            integer     NOT NULL,
  sequence            bigint      NOT NULL,
  assignment_epoch    bigint      NOT NULL,
  command_id          text        NOT NULL,
  request_id          text,
  request_fingerprint bytea,
  command_type        text        NOT NULL,
  item_ids            text[]      NOT NULL DEFAULT '{}',
  command_payload     jsonb       NOT NULL,
  checksum            bytea       NOT NULL,
  created_at          timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, queue_id, shard_id, sequence),
  UNIQUE (tenant_id, queue_id, shard_id, command_id)
);

CREATE INDEX IF NOT EXISTS pqueue_commands_replay_idx
  ON pqueue_commands (tenant_id, queue_id, shard_id, sequence);

CREATE TABLE IF NOT EXISTS pqueue_items (
  tenant_id             text        NOT NULL,
  queue_id              text        NOT NULL,
  shard_id              integer     NOT NULL,
  item_id               text        NOT NULL,
  client_item_key       text        NOT NULL,
  lifecycle_state       text        NOT NULL,
  priority              jsonb       NOT NULL,
  priority_sort         bytea       NOT NULL,
  not_before            timestamptz,
  eligible_since        timestamptz,
  group_key             text,
  cohort_size           integer,
  recurrence_until      timestamptz,
  payload               jsonb,
  metadata              jsonb       NOT NULL DEFAULT '{}'::jsonb,
  retry_count           integer     NOT NULL DEFAULT 0,
  retry_metadata        jsonb       NOT NULL DEFAULT '{}'::jsonb,
  failure_code          text,
  item_version          bigint      NOT NULL,
  lease_token_hash      bytea,
  lease_expires_at      timestamptz,
  worker_id             text,
  last_command_sequence bigint      NOT NULL,
  created_at            timestamptz NOT NULL DEFAULT now(),
  updated_at            timestamptz NOT NULL DEFAULT now(),
  terminal_at           timestamptz,
  PRIMARY KEY (tenant_id, queue_id, item_id),
  UNIQUE (tenant_id, queue_id, client_item_key)
);

CREATE INDEX IF NOT EXISTS pqueue_items_claim_strict_idx
  ON pqueue_items (
    tenant_id, queue_id, shard_id,
    lifecycle_state,
    priority_sort, created_at, item_id
  )
  WHERE lifecycle_state = 'pending';

CREATE INDEX IF NOT EXISTS pqueue_items_eligible_age_idx
  ON pqueue_items (
    tenant_id, queue_id, shard_id,
    lifecycle_state,
    eligible_since
  )
  WHERE lifecycle_state = 'pending';

CREATE INDEX IF NOT EXISTS pqueue_items_lease_expiry_idx
  ON pqueue_items (
    tenant_id, queue_id, shard_id,
    lifecycle_state,
    lease_expires_at
  )
  WHERE lifecycle_state = 'leased';

CREATE INDEX IF NOT EXISTS pqueue_items_group_claim_idx
  ON pqueue_items (
    tenant_id, queue_id, shard_id,
    group_key,
    priority_sort, created_at, item_id
  )
  WHERE lifecycle_state = 'pending';

CREATE TABLE IF NOT EXISTS pqueue_item_key_retention (
  tenant_id       text        NOT NULL,
  queue_id        text        NOT NULL,
  client_item_key text        NOT NULL,
  item_id         text        NOT NULL,
  expires_at      timestamptz NOT NULL,
  PRIMARY KEY (tenant_id, queue_id, client_item_key)
);

CREATE INDEX IF NOT EXISTS pqueue_item_key_retention_expiry_idx
  ON pqueue_item_key_retention (expires_at);

CREATE TABLE IF NOT EXISTS pqueue_request_idempotency (
  tenant_id           text        NOT NULL,
  queue_id            text        NOT NULL,
  operation           text        NOT NULL,
  request_id          text        NOT NULL,
  request_fingerprint bytea       NOT NULL,
  response_payload    jsonb,
  command_positions   jsonb       NOT NULL DEFAULT '{}'::jsonb,
  expires_at          timestamptz NOT NULL,
  created_at          timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, queue_id, operation, request_id)
);

CREATE INDEX IF NOT EXISTS pqueue_request_idempotency_expiry_idx
  ON pqueue_request_idempotency (expires_at);
"#;
