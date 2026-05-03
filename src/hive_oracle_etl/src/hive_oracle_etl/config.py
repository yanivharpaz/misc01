from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from dotenv import load_dotenv

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$#]*$")
_TABLE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$#]*(\.[A-Za-z_][A-Za-z0-9_$#]*)?$")
_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}


class ConfigError(ValueError):
    """Raised when required ETL configuration is missing or invalid."""


def _env(name: str, default: str | None = None, required: bool = False) -> str | None:
    value = os.getenv(name, default)
    if required and (value is None or value == ""):
        raise ConfigError(f"Missing required environment variable: {name}")
    return value


def _env_int(name: str, default: int) -> int:
    raw = _env(name, str(default))
    try:
        value = int(str(raw))
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer, got {raw!r}") from exc
    return value


def _env_bool(name: str, default: bool) -> bool:
    raw = str(_env(name, str(default))).strip().lower()
    if raw in _TRUE:
        return True
    if raw in _FALSE:
        return False
    raise ConfigError(f"{name} must be boolean, got {raw!r}")


def _split_csv(raw: str | None) -> list[str]:
    if raw is None or raw.strip() == "":
        return []
    return [part.strip() for part in raw.split(",") if part.strip()]


def _json_dict(name: str, default: dict[str, Any] | None = None) -> dict[str, Any]:
    raw = _env(name)
    if not raw:
        return default or {}
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ConfigError(f"{name} must be valid JSON") from exc
    if not isinstance(value, dict):
        raise ConfigError(f"{name} must be a JSON object")
    return value


def parse_datetime_utc(value: str, name: str) -> datetime:
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ConfigError(f"{name} must be ISO-8601 datetime, got {value!r}") from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def ensure_identifier(identifier: str, label: str) -> str:
    if not _IDENTIFIER_RE.match(identifier):
        raise ConfigError(
            f"{label} must be an unquoted SQL identifier. Got {identifier!r}. "
            "Use simple names like CUSTOMER_ID or created."
        )
    return identifier


def ensure_table_name(name: str, label: str) -> str:
    if not _TABLE_RE.match(name):
        raise ConfigError(
            f"{label} must be schema.table or table using unquoted SQL identifiers. Got {name!r}."
        )
    return name


def normalize_column_name(name: str) -> str:
    """Normalize returned DB column names by stripping table/database prefixes."""
    return name.split(".")[-1].strip()


@dataclass(frozen=True)
class EtlConfig:
    job_name: str

    hive_host: str
    hive_port: int
    hive_database: str
    hive_table: str
    hive_username: str | None
    hive_password: str | None
    hive_auth: str
    hive_kerberos_service_name: str | None
    hive_configuration: dict[str, Any]
    hive_created_column: str
    hive_select_columns: list[str]
    hive_extra_where: str | None

    oracle_user: str
    oracle_password: str
    oracle_dsn: str
    oracle_target_table: str
    oracle_stage_table: str
    oracle_control_table: str

    target_columns: list[str]
    key_columns: list[str]
    target_created_column: str
    column_renames: dict[str, str]
    uppercase_column_names: bool

    chunk_size: int
    insert_batch_size: int
    lookback_minutes: int
    safety_lag_minutes: int
    initial_watermark: datetime
    dry_run: bool
    log_level: str

    oracle_stmt_cache_size: int = 50
    extra_env: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_env(cls, env_file: str | None = None) -> "EtlConfig":
        if env_file:
            load_dotenv(env_file)
        else:
            load_dotenv()

        target_columns = [c.upper() for c in _split_csv(_env("TARGET_COLUMNS", required=True))]
        key_columns = [c.upper() for c in _split_csv(_env("KEY_COLUMNS", required=True))]
        if not target_columns:
            raise ConfigError("TARGET_COLUMNS must contain at least one column")
        if not key_columns:
            raise ConfigError("KEY_COLUMNS must contain at least one source business key column")

        target_created = str(_env("TARGET_CREATED_COLUMN", _env("HIVE_CREATED_COLUMN", "created"))).upper()
        select_columns = _split_csv(_env("HIVE_SELECT_COLUMNS")) or target_columns

        config = cls(
            job_name=str(_env("JOB_NAME", "hive_to_oracle_incremental")),
            hive_host=str(_env("HIVE_HOST", required=True)),
            hive_port=_env_int("HIVE_PORT", 10000),
            hive_database=str(_env("HIVE_DATABASE", "default")),
            hive_table=str(_env("HIVE_TABLE", required=True)),
            hive_username=_env("HIVE_USERNAME"),
            hive_password=_env("HIVE_PASSWORD"),
            hive_auth=str(_env("HIVE_AUTH", "NOSASL")),
            hive_kerberos_service_name=_env("HIVE_KERBEROS_SERVICE_NAME", "hive"),
            hive_configuration=_json_dict("HIVE_CONFIGURATION_JSON", {}),
            hive_created_column=str(_env("HIVE_CREATED_COLUMN", "created")),
            hive_select_columns=select_columns,
            hive_extra_where=_env("HIVE_EXTRA_WHERE"),
            oracle_user=str(_env("ORACLE_USER", required=True)),
            oracle_password=str(_env("ORACLE_PASSWORD", required=True)),
            oracle_dsn=str(_env("ORACLE_DSN", required=True)),
            oracle_target_table=str(_env("ORACLE_TARGET_TABLE", required=True)).upper(),
            oracle_stage_table=str(_env("ORACLE_STAGE_TABLE", required=True)).upper(),
            oracle_control_table=str(_env("ORACLE_CONTROL_TABLE", "ETL_WATERMARK")).upper(),
            target_columns=target_columns,
            key_columns=key_columns,
            target_created_column=target_created,
            column_renames={str(k): str(v) for k, v in _json_dict("COLUMN_RENAMES_JSON", {}).items()},
            uppercase_column_names=_env_bool("UPPERCASE_COLUMN_NAMES", True),
            chunk_size=_env_int("CHUNK_SIZE", 50000),
            insert_batch_size=_env_int("INSERT_BATCH_SIZE", 10000),
            lookback_minutes=_env_int("LOOKBACK_MINUTES", 180),
            safety_lag_minutes=_env_int("SAFETY_LAG_MINUTES", 10),
            initial_watermark=parse_datetime_utc(
                str(_env("INITIAL_WATERMARK", "1970-01-01T00:00:00+00:00")),
                "INITIAL_WATERMARK",
            ),
            dry_run=_env_bool("DRY_RUN", False),
            log_level=str(_env("LOG_LEVEL", "INFO")),
            oracle_stmt_cache_size=_env_int("ORACLE_STMT_CACHE_SIZE", 50),
        )
        config.validate()
        return config

    def validate(self) -> None:
        ensure_table_name(self.hive_table, "HIVE_TABLE")
        ensure_table_name(self.oracle_target_table, "ORACLE_TARGET_TABLE")
        ensure_table_name(self.oracle_stage_table, "ORACLE_STAGE_TABLE")
        ensure_table_name(self.oracle_control_table, "ORACLE_CONTROL_TABLE")
        ensure_identifier(self.hive_created_column, "HIVE_CREATED_COLUMN")
        ensure_identifier(self.target_created_column, "TARGET_CREATED_COLUMN")

        for col in self.hive_select_columns:
            ensure_identifier(col, "HIVE_SELECT_COLUMNS item")
        for col in self.target_columns:
            ensure_identifier(col, "TARGET_COLUMNS item")
        for col in self.key_columns:
            ensure_identifier(col, "KEY_COLUMNS item")

        missing_keys = sorted(set(self.key_columns) - set(self.target_columns))
        if missing_keys:
            raise ConfigError(f"KEY_COLUMNS are not present in TARGET_COLUMNS: {missing_keys}")
        if self.target_created_column not in self.target_columns:
            raise ConfigError(
                f"TARGET_CREATED_COLUMN {self.target_created_column!r} must be included in TARGET_COLUMNS"
            )
        if self.chunk_size <= 0 or self.insert_batch_size <= 0:
            raise ConfigError("CHUNK_SIZE and INSERT_BATCH_SIZE must be positive")
        if self.lookback_minutes < 0 or self.safety_lag_minutes < 0:
            raise ConfigError("LOOKBACK_MINUTES and SAFETY_LAG_MINUTES must be zero or positive")

    @property
    def source_table_fqn(self) -> str:
        return f"{self.hive_database}.{self.hive_table}" if "." not in self.hive_table else self.hive_table
