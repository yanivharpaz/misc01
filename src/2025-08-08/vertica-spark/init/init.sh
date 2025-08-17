#!/usr/bin/env bash
set -euo pipefail

VSQL="vsql -h vertica -U dbadmin -d VMart -At -v ON_ERROR_STOP=1"

echo "==> Checking/creating appuser"
USER_EXISTS=$($VSQL -c "SELECT COUNT(*) FROM v_catalog.users WHERE name = 'appuser';")
if [ "$USER_EXISTS" = "0" ]; then
  $VSQL -c "CREATE USER appuser IDENTIFIED BY 'appsecret';"
else
  # ensure password is what we expect, harmless if unchanged
  $VSQL -c "ALTER USER appuser IDENTIFIED BY 'appsecret';"
fi

echo "==> Checking/creating schema app"
SCHEMA_EXISTS=$($VSQL -c "SELECT COUNT(*) FROM v_catalog.schemata WHERE schema_name = 'app';")
if [ "$SCHEMA_EXISTS" = "0" ]; then
  # create owned by dbadmin first (always exists), then hand over to appuser
  $VSQL -c "CREATE SCHEMA app AUTHORIZATION dbadmin;"
fi
$VSQL -c "ALTER SCHEMA app OWNER TO appuser;"
$VSQL -c "GRANT USAGE ON SCHEMA app TO appuser;"
$VSQL -c "GRANT ALL ON DATABASE VMart TO appuser;"

echo "==> Creating sample table (idempotent)"
$VSQL -c "
CREATE TABLE IF NOT EXISTS app.events (
  id      IDENTITY,
  ts      TIMESTAMP DEFAULT NOW(),
  user_id VARCHAR(64),
  amount  NUMERIC(12,2),
  note    VARCHAR(200)
);"

echo "==> Seeding sample rows if table is empty"
ROWCOUNT=$($VSQL -c "SELECT COUNT(*) FROM app.events;")
if [ "$ROWCOUNT" = "0" ]; then
  $VSQL -c "
  INSERT /*+ DIRECT */ INTO app.events (user_id, amount, note) VALUES
    ('u1', 10.50, 'seed-1'),
    ('u2', 99.99, 'seed-2'),
    ('u3',  5.00, 'seed-3');"
fi

echo "==> Done"

