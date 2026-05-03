from __future__ import annotations

import logging

import pandas as pd

from hive_oracle_etl.config import EtlConfig, normalize_column_name

logger = logging.getLogger(__name__)


class DataValidationError(ValueError):
    """Raised when source data does not match target configuration."""


def _mapped_column_name(source_name: str, cfg: EtlConfig) -> str:
    normalized = normalize_column_name(str(source_name))
    mapped = (
        cfg.column_renames.get(normalized)
        or cfg.column_renames.get(normalized.lower())
        or cfg.column_renames.get(normalized.upper())
        or normalized
    )
    return mapped.upper() if cfg.uppercase_column_names else mapped


def transform_dataframe(df: pd.DataFrame, cfg: EtlConfig) -> pd.DataFrame:
    """Apply deterministic, idempotent transformations before Oracle loading.

    Keep this function side-effect free. Add business transformations here, but avoid
    non-deterministic logic such as "current timestamp" except ETL metadata columns,
    which are added separately by oracle.py.
    """
    if df.empty:
        return df

    out = df.copy()
    out.columns = [_mapped_column_name(c, cfg) for c in out.columns]

    # If the Hive query returned duplicate names after normalization, pandas allows it,
    # but Oracle loading would be ambiguous.
    duplicates = sorted({col for col in out.columns if list(out.columns).count(col) > 1})
    if duplicates:
        raise DataValidationError(f"Duplicate columns after normalization: {duplicates}")

    missing = sorted(set(cfg.target_columns) - set(out.columns))
    if missing:
        raise DataValidationError(
            f"Hive result is missing required target columns: {missing}. "
            "Check HIVE_SELECT_COLUMNS and COLUMN_RENAMES_JSON."
        )

    out = out[cfg.target_columns].copy()

    # Standardize the incremental column for Oracle inserts and comparisons.
    out[cfg.target_created_column] = pd.to_datetime(
        out[cfg.target_created_column],
        errors="raise",
        utc=True,
    ).dt.tz_convert("UTC").dt.tz_localize(None)

    # Convert pandas NA/NaT values to Python None before binding to Oracle.
    out = out.astype(object).where(pd.notnull(out), None)
    return out
