-- Oracle 19c DDL template for the Hive -> Oracle incremental ETL.
-- Replace APP_ETL, ORDERS, and business columns with your real schema/table.

-- 1) Target table. Adjust datatypes to match your Hive schema.
CREATE TABLE APP_ETL.ORDERS (
    ORDER_ID      NUMBER(19)       NOT NULL,
    CUSTOMER_ID   NUMBER(19),
    AMOUNT        NUMBER(18, 2),
    STATUS        VARCHAR2(30),
    CREATED       TIMESTAMP        NOT NULL,
    CONSTRAINT ORDERS_PK PRIMARY KEY (ORDER_ID)
);

-- Helpful when analysts query by created. The PK/unique key is what makes MERGE idempotent.
CREATE INDEX APP_ETL.ORDERS_CREATED_IX ON APP_ETL.ORDERS (CREATED);

-- 2) Persistent staging table. It contains the target columns plus ETL metadata.
-- The job deletes rows for its JOB_NAME at the start of each run and also cleans old rows.
CREATE TABLE APP_ETL.ORDERS_STG (
    ORDER_ID       NUMBER(19),
    CUSTOMER_ID    NUMBER(19),
    AMOUNT         NUMBER(18, 2),
    STATUS         VARCHAR2(30),
    CREATED        TIMESTAMP,
    ETL_RUN_ID     VARCHAR2(64)                 NOT NULL,
    ETL_JOB_NAME   VARCHAR2(128)                NOT NULL,
    ETL_LOADED_AT  TIMESTAMP WITH TIME ZONE     DEFAULT SYSTIMESTAMP NOT NULL
);

CREATE INDEX APP_ETL.ORDERS_STG_RUN_IX
    ON APP_ETL.ORDERS_STG (ETL_JOB_NAME, ETL_RUN_ID);

CREATE INDEX APP_ETL.ORDERS_STG_KEY_IX
    ON APP_ETL.ORDERS_STG (ORDER_ID);

-- 3) Control table for high-watermarks. One row per ETL job.
CREATE TABLE APP_ETL.ETL_WATERMARK (
    JOB_NAME          VARCHAR2(128)             NOT NULL,
    LAST_CREATED_TS   TIMESTAMP WITH TIME ZONE  NOT NULL,
    LAST_RUN_ID       VARCHAR2(64),
    LAST_SUCCESS_AT   TIMESTAMP WITH TIME ZONE,
    LAST_ROWS_READ    NUMBER,
    LAST_ROWS_MERGED  NUMBER,
    UPDATED_AT        TIMESTAMP WITH TIME ZONE  DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT ETL_WATERMARK_PK PRIMARY KEY (JOB_NAME)
);

COMMENT ON TABLE APP_ETL.ETL_WATERMARK IS 'Incremental ETL watermark state by job name.';
COMMENT ON COLUMN APP_ETL.ETL_WATERMARK.LAST_CREATED_TS IS 'Upper created timestamp processed by the last successful run.';

-- Optional: least-privilege grants if runtime user differs from owner.
-- GRANT SELECT, INSERT, UPDATE, DELETE ON APP_ETL.ORDERS TO APP_ETL_RUNTIME;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON APP_ETL.ORDERS_STG TO APP_ETL_RUNTIME;
-- GRANT SELECT, INSERT, UPDATE ON APP_ETL.ETL_WATERMARK TO APP_ETL_RUNTIME;
