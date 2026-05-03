from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Iterable, Sequence

import pandas as pd

from hive_oracle_etl.config import EtlConfig

logger = logging.getLogger(__name__)

ETL_METADATA_COLUMNS = ["ETL_RUN_ID", "ETL_JOB_NAME", "ETL_LOADED_AT"]


def connect_oracle(cfg: EtlConfig):
    try:
        import oracledb
    except ImportError as exc:  # pragma: no cover - deployment error path
        raise RuntimeError("python-oracledb is not installed. Check requirements.txt and Docker build logs.") from exc

    logger.info("opening Oracle connection", extra={"etl_oracle_dsn": cfg.oracle_dsn})
    return oracledb.connect(
        user=cfg.oracle_user,
        password=cfg.oracle_password,
        dsn=cfg.oracle_dsn,
        stmtcachesize=cfg.oracle_stmt_cache_size,
    )


def _to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _split_table_name(fqn: str, default_schema: str) -> tuple[str, str]:
    parts = fqn.upper().split(".")
    if len(parts) == 1:
        return default_schema.upper(), parts[0]
    return parts[0], parts[1]


def get_watermark(connection, cfg: EtlConfig) -> datetime:
    sql = f"SELECT last_created_ts FROM {cfg.oracle_control_table} WHERE job_name = :job_name"
    with connection.cursor() as cursor:
        cursor.execute(sql, job_name=cfg.job_name)
        row = cursor.fetchone()
        if row:
            return _to_utc(row[0])

        insert_sql = f"""
            INSERT INTO {cfg.oracle_control_table}
                (job_name, last_created_ts, last_run_id, last_success_at, last_rows_read,
                 last_rows_merged, updated_at)
            VALUES
                (:job_name, :last_created_ts, NULL, NULL, 0, 0, SYSTIMESTAMP)
        """
        cursor.execute(insert_sql, job_name=cfg.job_name, last_created_ts=cfg.initial_watermark)
    connection.commit()
    logger.info("initialized ETL watermark", extra={"etl_watermark": cfg.initial_watermark.isoformat()})
    return cfg.initial_watermark


def update_watermark(
    connection,
    cfg: EtlConfig,
    new_watermark: datetime,
    run_id: str,
    rows_read: int,
    rows_merged: int,
) -> None:
    sql = f"""
        MERGE INTO {cfg.oracle_control_table} c
        USING (SELECT :job_name AS job_name FROM dual) s
        ON (c.job_name = s.job_name)
        WHEN MATCHED THEN UPDATE SET
            c.last_created_ts = :last_created_ts,
            c.last_run_id = :run_id,
            c.last_success_at = SYSTIMESTAMP,
            c.last_rows_read = :rows_read,
            c.last_rows_merged = :rows_merged,
            c.updated_at = SYSTIMESTAMP
        WHEN NOT MATCHED THEN INSERT
            (job_name, last_created_ts, last_run_id, last_success_at, last_rows_read,
             last_rows_merged, updated_at)
        VALUES
            (:job_name, :last_created_ts, :run_id, SYSTIMESTAMP, :rows_read,
             :rows_merged, SYSTIMESTAMP)
    """
    with connection.cursor() as cursor:
        cursor.execute(
            sql,
            job_name=cfg.job_name,
            last_created_ts=_to_utc(new_watermark),
            run_id=run_id,
            rows_read=rows_read,
            rows_merged=rows_merged,
        )


def delete_stage_rows(connection, cfg: EtlConfig) -> int:
    sql = f"DELETE FROM {cfg.oracle_stage_table} WHERE etl_job_name = :job_name"
    with connection.cursor() as cursor:
        cursor.execute(sql, job_name=cfg.job_name)
        deleted = cursor.rowcount or 0
    logger.info("deleted stale stage rows", extra={"etl_rows": deleted})
    return deleted


def cleanup_old_stage_rows(connection, cfg: EtlConfig, days_to_keep: int = 7) -> int:
    sql = f"""
        DELETE FROM {cfg.oracle_stage_table}
        WHERE etl_job_name = :job_name
          AND etl_loaded_at < SYSTIMESTAMP - NUMTODSINTERVAL(:days_to_keep, 'DAY')
    """
    with connection.cursor() as cursor:
        cursor.execute(sql, job_name=cfg.job_name, days_to_keep=days_to_keep)
        deleted = cursor.rowcount or 0
    logger.info("deleted old stage rows", extra={"etl_rows": deleted})
    return deleted


def _clean_scalar(value):
    if value is None:
        return None
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None
        return value.to_pydatetime()
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "item"):
        try:
            return value.item()
        except (ValueError, TypeError):
            return value
    return value


def _dataframe_records(df: pd.DataFrame, columns: Sequence[str]) -> list[tuple]:
    return [tuple(_clean_scalar(value) for value in row) for row in df.loc[:, columns].itertuples(index=False, name=None)]


def _chunks(values: Sequence[tuple], size: int) -> Iterable[Sequence[tuple]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


def insert_stage_dataframe(connection, cfg: EtlConfig, df: pd.DataFrame, run_id: str) -> int:
    if df.empty:
        return 0

    load_df = df.copy()
    load_df["ETL_RUN_ID"] = run_id
    load_df["ETL_JOB_NAME"] = cfg.job_name
    load_df["ETL_LOADED_AT"] = datetime.now(timezone.utc)

    stage_columns = cfg.target_columns + ETL_METADATA_COLUMNS
    bind_placeholders = ", ".join([f":{idx}" for idx in range(1, len(stage_columns) + 1)])
    column_list = ", ".join(stage_columns)
    sql = f"INSERT INTO {cfg.oracle_stage_table} ({column_list}) VALUES ({bind_placeholders})"
    rows = _dataframe_records(load_df, stage_columns)

    with connection.cursor() as cursor:
        for batch in _chunks(rows, cfg.insert_batch_size):
            cursor.executemany(sql, list(batch))

    logger.info("inserted stage chunk", extra={"etl_rows": len(rows), "etl_run_id": run_id})
    return len(rows)


def merge_stage_to_target(connection, cfg: EtlConfig, run_id: str) -> int:
    target_cols = cfg.target_columns
    key_cols = cfg.key_columns
    update_cols = [col for col in target_cols if col not in set(key_cols)]

    source_col_list = ", ".join(target_cols)
    partition_by = ", ".join(key_cols)
    on_clause = " AND ".join([f"t.{col} = s.{col}" for col in key_cols])
    insert_cols = ", ".join(target_cols)
    insert_values = ", ".join([f"s.{col}" for col in target_cols])

    matched_clause = ""
    if update_cols:
        update_set = ",\n            ".join([f"t.{col} = s.{col}" for col in update_cols])
        matched_clause = f"""
        WHEN MATCHED THEN UPDATE SET
            {update_set}
        """

    sql = f"""
        MERGE INTO {cfg.oracle_target_table} t
        USING (
            SELECT {source_col_list}
            FROM (
                SELECT {source_col_list},
                       ROW_NUMBER() OVER (
                           PARTITION BY {partition_by}
                           ORDER BY {cfg.target_created_column} DESC, etl_loaded_at DESC
                       ) AS rn
                FROM {cfg.oracle_stage_table}
                WHERE etl_run_id = :run_id
                  AND etl_job_name = :job_name
            )
            WHERE rn = 1
        ) s
        ON ({on_clause})
        {matched_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_cols})
        VALUES ({insert_values})
    """
    with connection.cursor() as cursor:
        cursor.execute(sql, run_id=run_id, job_name=cfg.job_name)
        affected = cursor.rowcount or 0
    logger.info("merged stage into target", extra={"etl_rows": affected, "etl_run_id": run_id})
    return affected


def table_columns(connection, cfg: EtlConfig, table_name: str) -> set[str]:
    owner, table = _split_table_name(table_name, cfg.oracle_user)
    sql = """
        SELECT column_name
        FROM all_tab_columns
        WHERE owner = :owner
          AND table_name = :table_name
    """
    with connection.cursor() as cursor:
        cursor.execute(sql, owner=owner.upper(), table_name=table.upper())
        return {row[0].upper() for row in cursor.fetchall()}


def preflight_oracle_metadata(connection, cfg: EtlConfig) -> None:
    target_columns = table_columns(connection, cfg, cfg.oracle_target_table)
    stage_columns = table_columns(connection, cfg, cfg.oracle_stage_table)

    if not target_columns:
        raise RuntimeError(f"Target table not found or not visible: {cfg.oracle_target_table}")
    if not stage_columns:
        raise RuntimeError(f"Stage table not found or not visible: {cfg.oracle_stage_table}")

    missing_target = sorted(set(cfg.target_columns) - target_columns)
    missing_stage = sorted(set(cfg.target_columns + ETL_METADATA_COLUMNS) - stage_columns)
    if missing_target:
        raise RuntimeError(f"Target table is missing columns: {missing_target}")
    if missing_stage:
        raise RuntimeError(f"Stage table is missing columns: {missing_stage}")

    logger.info("Oracle metadata preflight passed")
