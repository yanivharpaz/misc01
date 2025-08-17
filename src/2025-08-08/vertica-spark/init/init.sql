-- Runs once (via the vertica-init service) to set up your sandbox

-- Create a dedicated user + schema
CREATE USER IF NOT EXISTS appuser IDENTIFIED BY 'appsecret';
GRANT ALL ON DATABASE VMart TO appuser;

CREATE SCHEMA IF NOT EXISTS app AUTHORIZATION appuser;
GRANT USAGE ON SCHEMA app TO appuser;

-- Sample table
CREATE TABLE IF NOT EXISTS app.events (
  id            IDENTITY,
  ts            TIMESTAMP DEFAULT NOW(),
  user_id       VARCHAR(64),
  amount        NUMERIC(12,2),
  note          VARCHAR(200)
);

-- Some seed rows
INSERT /*+ DIRECT */ INTO app.events (user_id, amount, note)
VALUES
  ('u1', 10.50, 'seed-1'),
  ('u2', 99.99, 'seed-2'),
  ('u3',  5.00, 'seed-3');

COMMIT;

