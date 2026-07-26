CREATE TABLE IF NOT EXISTS payments (
  payment_id UUID PRIMARY KEY,
  merchant_id UUID NOT NULL,
  amount_cents BIGINT NOT NULL CHECK (amount_cents > 0),
  fee_bps INTEGER NOT NULL CHECK (fee_bps BETWEEN 0 AND 10000),
  fee_cents BIGINT NOT NULL CHECK (fee_cents >= 0),
  merchant_net_cents BIGINT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('AUTHORIZED','CAPTURED','PARTIALLY_REFUNDED','REFUNDED')),
  captured_cents BIGINT NOT NULL DEFAULT 0,
  refunded_cents BIGINT NOT NULL DEFAULT 0,
  code_version TEXT NOT NULL,
  config_version TEXT NOT NULL,
  row_version BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CHECK (merchant_net_cents = captured_cents - refunded_cents - fee_cents),
  CHECK (captured_cents BETWEEN 0 AND amount_cents),
  CHECK (refunded_cents BETWEEN 0 AND captured_cents)
);

CREATE TABLE IF NOT EXISTS ledger_entries (
  entry_id UUID PRIMARY KEY,
  payment_id UUID NOT NULL REFERENCES payments(payment_id),
  account TEXT NOT NULL CHECK (account IN ('CUSTOMER','MERCHANT','PLATFORM_FEE')),
  amount_cents BIGINT NOT NULL,
  entry_kind TEXT NOT NULL,
  repair_job_id UUID,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (payment_id, account, entry_kind, repair_job_id)
);

CREATE TABLE IF NOT EXISTS outbox_events (
  sequence_id BIGSERIAL PRIMARY KEY,
  event_id UUID NOT NULL UNIQUE,
  aggregate_id UUID NOT NULL,
  event_type TEXT NOT NULL,
  source_txid BIGINT NOT NULL DEFAULT txid_current(),
  code_version TEXT NOT NULL,
  config_version TEXT NOT NULL,
  payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  published_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS record_lineage (
  event_id UUID PRIMARY KEY,
  record_id UUID NOT NULL,
  event_type TEXT NOT NULL,
  source_txid BIGINT NOT NULL,
  code_version TEXT NOT NULL,
  config_version TEXT NOT NULL,
  payload JSONB NOT NULL,
  kafka_partition INTEGER NOT NULL,
  kafka_offset BIGINT NOT NULL,
  observed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS lineage_record_idx ON record_lineage(record_id);
CREATE INDEX IF NOT EXISTS lineage_version_idx ON record_lineage(config_version, code_version);

CREATE TABLE IF NOT EXISTS repair_jobs (
  job_id UUID PRIMARY KEY,
  bad_config_version TEXT NOT NULL,
  target_fee_bps INTEGER NOT NULL,
  plan_hash TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('PLANNED','RUNNING','COMPLETED','COMPLETED_WITH_CONFLICTS','FAILED','ROLLED_BACK')),
  dry_run BOOLEAN NOT NULL,
  affected_count BIGINT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  started_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ
);

CREATE OR REPLACE FUNCTION dry_run_job(candidate UUID) RETURNS BOOLEAN
LANGUAGE SQL STABLE AS $$
  SELECT dry_run FROM repair_jobs WHERE job_id=candidate
$$;

CREATE TABLE IF NOT EXISTS repair_items (
  job_id UUID NOT NULL REFERENCES repair_jobs(job_id),
  payment_id UUID NOT NULL REFERENCES payments(payment_id),
  repair_key TEXT NOT NULL UNIQUE,
  partition_id INTEGER NOT NULL,
  expected_row_version BIGINT NOT NULL,
  before_image JSONB NOT NULL,
  after_image JSONB NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('PENDING','APPLYING','APPLIED','CONFLICT','ROLLED_BACK','FAILED')),
  attempt_count INTEGER NOT NULL DEFAULT 0,
  worker_id TEXT,
  claimed_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ,
  error TEXT,
  PRIMARY KEY (job_id, payment_id)
);
CREATE INDEX IF NOT EXISTS repair_work_idx ON repair_items(job_id, state, partition_id);

CREATE TABLE IF NOT EXISTS repair_checkpoints (
  job_id UUID NOT NULL REFERENCES repair_jobs(job_id),
  partition_id INTEGER NOT NULL,
  completed_count BIGINT NOT NULL DEFAULT 0,
  last_payment_id UUID,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (job_id, partition_id)
);

CREATE TABLE IF NOT EXISTS audit_log (
  audit_id BIGSERIAL PRIMARY KEY,
  job_id UUID,
  payment_id UUID,
  action TEXT NOT NULL,
  worker_id TEXT,
  details JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS audit_job_idx ON audit_log(job_id, audit_id);

CREATE OR REPLACE FUNCTION forbid_audit_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'audit_log is append-only';
END $$;
DROP TRIGGER IF EXISTS audit_append_only ON audit_log;
CREATE TRIGGER audit_append_only BEFORE UPDATE OR DELETE ON audit_log
FOR EACH ROW EXECUTE FUNCTION forbid_audit_mutation();
