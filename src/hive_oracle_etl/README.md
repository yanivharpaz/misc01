# Hive to Oracle 19c Incremental ETL

A production-oriented Python project for loading daily increments from a Hadoop/Hive table into Oracle Database 19c from an OpenShift pod.

The source table is filtered by a Hive timestamp column named `created`. The target is loaded through an Oracle staging table and an idempotent `MERGE` into the final target table.

## Why this design

Daily volume is about 200,000 rows, so a single Spark application is probably unnecessary. This project uses a lightweight Python pod and processes rows in chunks with pandas DataFrames.

The key parts are:

1. **Half-open incremental window**: `created >= lower_bound AND created < upper_bound`.
2. **Oracle-controlled watermark**: the last successful upper bound is stored in `ETL_WATERMARK`.
3. **Lookback window**: each run re-reads a configurable number of previous minutes to catch late-arriving Hive rows.
4. **Idempotent Oracle MERGE**: duplicates from the lookback are harmless when `KEY_COLUMNS` uniquely identify a source row.
5. **Safety lag**: the most recent few minutes are skipped to avoid reading rows still being written in Hive.
6. **OpenShift CronJob**: the job runs on a schedule with `concurrencyPolicy: Forbid` to avoid overlapping runs.

## Project layout

```text
hive_oracle_etl/
  Dockerfile
  Makefile
  README.md
  pyproject.toml
  requirements.txt
  requirements-dev.txt
  .env.example
  src/hive_oracle_etl/
    __init__.py
    config.py
    hive.py
    logging_config.py
    main.py
    oracle.py
    transform.py
  sql/
    oracle_ddl.sql
    oracle_drop_dev.sql
  deploy/openshift/
    serviceaccount.yaml
    configmap.yaml
    secret.yaml
    cronjob.yaml
    networkpolicy-egress.yaml
    kustomization.yaml
  scripts/
    run_local.sh
    oc_deploy.sh
    run_once_from_cronjob.sh
  tests/
    test_transform.py
```

## Required Oracle objects

Run `sql/oracle_ddl.sql` after adapting schema, table names, columns, datatypes, and key constraints.

The project expects three Oracle objects:

| Object | Purpose |
|---|---|
| Target table | Final business table, for example `APP_ETL.ORDERS`. |
| Stage table | Same business columns as target plus `ETL_RUN_ID`, `ETL_JOB_NAME`, `ETL_LOADED_AT`. |
| Watermark table | One row per job in `ETL_WATERMARK`. |

A stable source key is required. With only `created`, an ETL can know *when* to read but cannot safely know whether a row is new, duplicated, or updated. Configure `KEY_COLUMNS` with the business key or source primary key, for example `ORDER_ID`.

## Configuration

Copy `.env.example` to `.env` for local testing, or use the OpenShift ConfigMap and Secret.

Important variables:

| Variable | Example | Notes |
|---|---:|---|
| `HIVE_HOST` | `hiveserver2.example.com` | HiveServer2 host. |
| `HIVE_PORT` | `10000` | HiveServer2 port. |
| `HIVE_DATABASE` | `sales_dw` | Hive database/schema. |
| `HIVE_TABLE` | `orders` | Hive source table. |
| `HIVE_CREATED_COLUMN` | `created` | Incremental source column. |
| `HIVE_SELECT_COLUMNS` | `order_id,customer_id,amount,status,created` | Source columns to select. Defaults to `TARGET_COLUMNS`. |
| `ORACLE_DSN` | `oracle19c.example.com:1521/ORCLPDB1` | Oracle Easy Connect string. |
| `ORACLE_TARGET_TABLE` | `APP_ETL.ORDERS` | Final table. |
| `ORACLE_STAGE_TABLE` | `APP_ETL.ORDERS_STG` | Persistent stage table. |
| `TARGET_COLUMNS` | `ORDER_ID,CUSTOMER_ID,AMOUNT,STATUS,CREATED` | Oracle target columns in load order. |
| `KEY_COLUMNS` | `ORDER_ID` | Columns used in Oracle `MERGE ON`. |
| `LOOKBACK_MINUTES` | `180` | Re-read recent history to catch late rows. |
| `SAFETY_LAG_MINUTES` | `10` | Avoid currently written Hive data. |
| `CHUNK_SIZE` | `50000` | Number of Hive rows per pandas DataFrame. |
| `INSERT_BATCH_SIZE` | `10000` | Number of Oracle rows per `executemany` batch. |

## Local development

```bash
python -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements-dev.txt
cp .env.example .env
# edit .env
python -m hive_oracle_etl.main --env-file .env --dry-run --print-query
python -m hive_oracle_etl.main --env-file .env --print-query
```

Run tests:

```bash
python -m pytest -q
```

## Container build

With Docker or Podman:

```bash
docker build -t hive-oracle-etl:0.1.0 .
# or
podman build -t hive-oracle-etl:0.1.0 .
```

The Dockerfile is OpenShift-friendly: it does not require a fixed runtime UID and makes `/app` and `/tmp/etl` writable by the root group.

## OpenShift deployment

1. Edit `deploy/openshift/configmap.yaml`.
2. Edit `deploy/openshift/secret.yaml` or create the Secret separately from your secret manager.
3. Edit the image in `deploy/openshift/cronjob.yaml`.
4. Apply the manifests:

```bash
oc project YOUR_PROJECT
oc apply -k deploy/openshift
```

Run once manually from the CronJob:

```bash
oc create job hive-oracle-etl-manual-$(date +%Y%m%d%H%M%S) --from=cronjob/hive-oracle-etl
oc logs -f job/<created-job-name>
```

## Kerberos notes

If HiveServer2 uses Kerberos:

1. Set `HIVE_AUTH=KERBEROS` and `HIVE_KERBEROS_SERVICE_NAME=hive`.
2. Mount a `krb5.conf` ConfigMap and a keytab Secret.
3. Run `kinit -kt /path/to/keytab principal` before starting Python, usually by wrapping the container command or using an init container.
4. Ensure the OpenShift pod can reach KDC, HiveServer2, and Oracle through network policy/firewall rules.

The base Dockerfile already includes Kerberos and SASL operating-system packages, but your realm, keytab, and principal are environment-specific.

## Operational behavior

On each run, the application:

1. Reads the previous `LAST_CREATED_TS` from Oracle `ETL_WATERMARK`.
2. Computes:
   - `lower_bound = last_watermark - LOOKBACK_MINUTES`
   - `upper_bound = now_utc - SAFETY_LAG_MINUTES`
3. Queries Hive with `created >= lower_bound AND created < upper_bound`.
4. Fetches source rows in `CHUNK_SIZE` batches.
5. Normalizes columns and converts `created` to UTC-naive Python datetimes for Oracle `TIMESTAMP` columns.
6. Inserts chunks into the Oracle stage table with `executemany`.
7. Runs one Oracle `MERGE` from stage to target.
8. Updates the watermark to `upper_bound` only after the merge succeeds.

If the job fails before watermark update, the next run will delete stale stage rows for the same `JOB_NAME` and retry the same interval, including the lookback.

## Custom transformations

Add deterministic business logic in `src/hive_oracle_etl/transform.py`.

Examples:

```python
out["STATUS"] = out["STATUS"].str.upper()
out["AMOUNT"] = pd.to_numeric(out["AMOUNT"], errors="raise")
```

Avoid non-deterministic transformations in this function. Runtime metadata is already handled separately by `oracle.py`.

## Tuning starting points

For 200k rows/day:

- `CHUNK_SIZE=50000` usually means about four chunks per daily run.
- `INSERT_BATCH_SIZE=10000` is a practical starting point for Oracle bulk inserts.
- Increase OpenShift memory if your rows are wide.
- Add an index or partition on Hive `created` if your Hive layout supports it.
- Keep a primary key or unique constraint on the Oracle target key columns.

## Limitations to review before production

- You need a stable `KEY_COLUMNS` definition. Without a key, idempotent loading cannot be guaranteed.
- If Hive receives rows whose `created` value is older than `LOOKBACK_MINUTES`, they will not be picked up after the lookback expires.
- The PyHive connection parameters vary by Hadoop distribution. If your organization exposes Hive only through Knox, JDBC, or ODBC, replace `hive.py` with the approved client while keeping the rest of the project structure.
- Oracle datatypes in `sql/oracle_ddl.sql` are examples. Match precision, scale, timestamps, and string lengths to your real schema.
