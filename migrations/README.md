# Tenant Data Migrations

Place tenant data migrations for this Lambda in this directory.

## Naming

Use a numeric prefix so the CLI can apply files in a deterministic order:

```text
001_create_example_records.sql
002_add_status_to_example_records.sql
```

## Idempotency

Migrations must be safe to re-run. `chask function publish` calls the schema/grants endpoint on every tenant-data publish, and the backend uses a deterministic migration id to make unchanged publishes a fast no-op.

Use idempotent DDL:

```sql
CREATE TABLE IF NOT EXISTS example_records (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  org_id uuid NOT NULL,
  title text NOT NULL,
  status text NOT NULL DEFAULT 'new',
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE example_records ADD COLUMN IF NOT EXISTS notes text;
CREATE INDEX IF NOT EXISTS idx_example_records_org_id ON example_records (org_id);
```

Avoid destructive DDL such as `DROP DATABASE`, `DROP SCHEMA`, `TRUNCATE`, or shell/file import operations.

## Manifest Alignment

Every table created here must be declared in `manifest.yml` under `function.data`, and every table declared in `function.data` must be created by a migration.

```yaml
function:
  data:
    writes:
      - table: example_records
        mode: insert
        columns: [title, status]
    reads:
      - table: example_records
        columns: [id, title, status]
    rpc:
      - GET /api/test/example-records/list
```

Declare only the tables and columns this Lambda actually needs. The publish flow uses this contract to create the Lambda permission version before the Lambda is activated.
