from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Iterator

import pandas as pd

from hive_oracle_etl.config import EtlConfig

logger = logging.getLogger(__name__)


def _hive_timestamp_literal(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt_utc = dt.astimezone(timezone.utc)
    return dt_utc.strftime("%Y-%m-%d %H:%M:%S.%f")


def build_incremental_query(cfg: EtlConfig, lower_bound: datetime, upper_bound: datetime) -> str:
    columns = ", ".join(cfg.hive_select_columns)
    lower = _hive_timestamp_literal(lower_bound)
    upper = _hive_timestamp_literal(upper_bound)

    predicates = [
        f"{cfg.hive_created_column} >= CAST('{lower}' AS TIMESTAMP)",
        f"{cfg.hive_created_column} < CAST('{upper}' AS TIMESTAMP)",
    ]
    if cfg.hive_extra_where:
        predicates.append(f"({cfg.hive_extra_where})")

    where_clause = "\n  AND ".join(predicates)
    return f"""SELECT {columns}
FROM {cfg.source_table_fqn}
WHERE {where_clause}"""


def connect_hive(cfg: EtlConfig):
    """Create a HiveServer2 DB-API connection using PyHive.

    Common HIVE_AUTH values: NOSASL, NONE, LDAP, KERBEROS, CUSTOM.
    Exact auth support depends on the HiveServer2 distribution and security setup.
    """
    try:
        from pyhive import hive
    except ImportError as exc:  # pragma: no cover - deployment error path
        raise RuntimeError("PyHive is not installed. Check requirements.txt and Docker build logs.") from exc

    kwargs = {
        "host": cfg.hive_host,
        "port": cfg.hive_port,
        "username": cfg.hive_username,
        "database": cfg.hive_database,
        "auth": cfg.hive_auth,
        "configuration": cfg.hive_configuration or None,
    }
    if cfg.hive_password:
        kwargs["password"] = cfg.hive_password
    if cfg.hive_auth.upper() == "KERBEROS" and cfg.hive_kerberos_service_name:
        kwargs["kerberos_service_name"] = cfg.hive_kerberos_service_name

    logger.info(
        "opening HiveServer2 connection",
        extra={"etl_hive_host": cfg.hive_host, "etl_hive_port": cfg.hive_port, "etl_hive_database": cfg.hive_database},
    )
    return hive.Connection(**kwargs)


def read_hive_in_chunks(connection, query: str, chunk_size: int) -> Iterator[pd.DataFrame]:
    """Execute Hive SQL and yield pandas DataFrames using cursor.fetchmany()."""
    cursor = connection.cursor()
    try:
        cursor.arraysize = chunk_size
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        chunk_number = 0
        while True:
            rows = cursor.fetchmany(chunk_size)
            if not rows:
                break
            chunk_number += 1
            df = pd.DataFrame.from_records(rows, columns=columns)
            logger.info(
                "fetched Hive chunk",
                extra={"etl_chunk_number": chunk_number, "etl_rows": len(df)},
            )
            yield df
    finally:
        cursor.close()
