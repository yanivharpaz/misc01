from __future__ import annotations

import argparse
import logging
import sys
import uuid
from contextlib import closing
from dataclasses import replace
from datetime import datetime, timedelta, timezone

from hive_oracle_etl.config import ConfigError, EtlConfig
from hive_oracle_etl.hive import build_incremental_query, connect_hive, read_hive_in_chunks
from hive_oracle_etl.logging_config import setup_logging
from hive_oracle_etl.oracle import (
    cleanup_old_stage_rows,
    connect_oracle,
    delete_stage_rows,
    get_watermark,
    insert_stage_dataframe,
    merge_stage_to_target,
    preflight_oracle_metadata,
    update_watermark,
)
from hive_oracle_etl.transform import transform_dataframe

logger = logging.getLogger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Incrementally load Hive rows into Oracle 19c")
    parser.add_argument("--env-file", help="Path to a local .env file. In OpenShift, use env vars instead.")
    parser.add_argument("--dry-run", action="store_true", help="Build the query and execution window without connecting.")
    parser.add_argument("--print-query", action="store_true", help="Print generated Hive SQL.")
    parser.add_argument("--skip-preflight", action="store_true", help="Skip Oracle table metadata validation.")
    return parser.parse_args(argv)


def compute_window(cfg: EtlConfig, last_watermark: datetime) -> tuple[datetime, datetime]:
    lower_bound = last_watermark - timedelta(minutes=cfg.lookback_minutes)
    upper_bound = datetime.now(timezone.utc) - timedelta(minutes=cfg.safety_lag_minutes)
    return lower_bound, upper_bound


def run(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    cfg = EtlConfig.from_env(args.env_file)
    if args.dry_run:
        cfg = replace(cfg, dry_run=True)

    setup_logging(cfg.log_level)
    run_id = uuid.uuid4().hex

    if cfg.dry_run:
        lower_bound, upper_bound = compute_window(cfg, cfg.initial_watermark)
        query = build_incremental_query(cfg, lower_bound, upper_bound)
        logger.info(
            "dry run generated execution window",
            extra={
                "etl_run_id": run_id,
                "etl_lower_bound": lower_bound.isoformat(),
                "etl_upper_bound": upper_bound.isoformat(),
            },
        )
        if args.print_query:
            print(query)
        return 0

    oracle_conn = None
    hive_conn = None
    try:
        oracle_conn = connect_oracle(cfg)
        if not args.skip_preflight:
            preflight_oracle_metadata(oracle_conn, cfg)

        last_watermark = get_watermark(oracle_conn, cfg)
        lower_bound, upper_bound = compute_window(cfg, last_watermark)
        if upper_bound <= lower_bound:
            logger.warning(
                "empty or invalid execution window; nothing to do",
                extra={
                    "etl_run_id": run_id,
                    "etl_lower_bound": lower_bound.isoformat(),
                    "etl_upper_bound": upper_bound.isoformat(),
                },
            )
            return 0

        query = build_incremental_query(cfg, lower_bound, upper_bound)
        logger.info(
            "starting ETL run",
            extra={
                "etl_run_id": run_id,
                "etl_last_watermark": last_watermark.isoformat(),
                "etl_lower_bound": lower_bound.isoformat(),
                "etl_upper_bound": upper_bound.isoformat(),
            },
        )
        if args.print_query:
            print(query)

        delete_stage_rows(oracle_conn, cfg)
        oracle_conn.commit()

        rows_read = 0
        hive_conn = connect_hive(cfg)
        with closing(hive_conn):
            for chunk in read_hive_in_chunks(hive_conn, query, cfg.chunk_size):
                transformed = transform_dataframe(chunk, cfg)
                if transformed.empty:
                    continue
                rows_read += len(transformed)
                insert_stage_dataframe(oracle_conn, cfg, transformed, run_id)
                oracle_conn.commit()

        rows_merged = merge_stage_to_target(oracle_conn, cfg, run_id)
        update_watermark(oracle_conn, cfg, upper_bound, run_id, rows_read, rows_merged)
        cleanup_old_stage_rows(oracle_conn, cfg)
        oracle_conn.commit()

        logger.info(
            "ETL run finished successfully",
            extra={"etl_run_id": run_id, "etl_rows_read": rows_read, "etl_rows_merged": rows_merged},
        )
        return 0
    except Exception:
        logger.exception("ETL run failed", extra={"etl_run_id": run_id})
        if oracle_conn is not None:
            try:
                oracle_conn.rollback()
            except Exception:  # pragma: no cover - best effort cleanup
                logger.exception("Oracle rollback failed", extra={"etl_run_id": run_id})
        return 1
    finally:
        if oracle_conn is not None:
            oracle_conn.close()


def main() -> None:
    try:
        raise SystemExit(run())
    except ConfigError as exc:
        setup_logging("INFO")
        logger.error("configuration error", extra={"etl_error": str(exc)})
        raise SystemExit(2) from exc


if __name__ == "__main__":
    main()
