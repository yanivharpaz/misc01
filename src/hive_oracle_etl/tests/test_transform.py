from datetime import datetime, timezone

import pandas as pd

from hive_oracle_etl.config import EtlConfig
from hive_oracle_etl.hive import build_incremental_query
from hive_oracle_etl.transform import transform_dataframe


def cfg() -> EtlConfig:
    return EtlConfig(
        job_name="test_job",
        hive_host="hive.example.com",
        hive_port=10000,
        hive_database="default",
        hive_table="source_table",
        hive_username="u",
        hive_password="p",
        hive_auth="NOSASL",
        hive_kerberos_service_name="hive",
        hive_configuration={},
        hive_created_column="created",
        hive_select_columns=["id", "created", "name"],
        hive_extra_where=None,
        oracle_user="APP",
        oracle_password="secret",
        oracle_dsn="dbhost:1521/ORCLPDB1",
        oracle_target_table="APP.TARGET_TABLE",
        oracle_stage_table="APP.TARGET_TABLE_STG",
        oracle_control_table="APP.ETL_WATERMARK",
        target_columns=["ID", "CREATED", "NAME"],
        key_columns=["ID"],
        target_created_column="CREATED",
        column_renames={},
        uppercase_column_names=True,
        chunk_size=50000,
        insert_batch_size=10000,
        lookback_minutes=180,
        safety_lag_minutes=10,
        initial_watermark=datetime(1970, 1, 1, tzinfo=timezone.utc),
        dry_run=False,
        log_level="INFO",
    )


def test_transform_normalizes_columns_and_created_timestamp():
    frame = pd.DataFrame(
        {
            "id": [1],
            "created": ["2026-05-03T10:15:30Z"],
            "name": ["alice"],
        }
    )
    out = transform_dataframe(frame, cfg())
    assert list(out.columns) == ["ID", "CREATED", "NAME"]
    assert out.loc[0, "CREATED"].tzinfo is None


def test_build_incremental_query_uses_half_open_window():
    query = build_incremental_query(
        cfg(),
        datetime(2026, 5, 1, 0, 0, tzinfo=timezone.utc),
        datetime(2026, 5, 2, 0, 0, tzinfo=timezone.utc),
    )
    assert "created >= CAST('2026-05-01 00:00:00.000000' AS TIMESTAMP)" in query
    assert "created < CAST('2026-05-02 00:00:00.000000' AS TIMESTAMP)" in query
