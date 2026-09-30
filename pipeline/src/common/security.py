"""Data Privacy & PII Protection Utilities for PySpark Pipelines."""
import os

from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# Salt is externalized so it can be rotated per environment without a code change.
# A production deployment MUST set PII_TOKENIZATION_SALT (kept in .env / secret store).
DEFAULT_SALT = os.environ.get("PII_TOKENIZATION_SALT", "credit_risk_salt_2026")


def mask_string(
    col: Column,
    keep_prefix: int = 1,
    keep_suffix: int = 1,
    mask_char: str = "*",
    mask_length: int = 4,
) -> Column:
    """
    Masks a string column, preserving leading and trailing characters.
    Example: '0912345678' -> '0****8'
    """
    col_str = F.coalesce(col.cast(StringType()), F.lit(""))
    str_len = F.length(col_str)

    prefix = F.substring(col_str, 1, keep_prefix)
    suffix = F.substring(col_str, -keep_suffix, keep_suffix)
    mask_repeated = F.repeat(F.lit(mask_char), mask_length)

    return (
        F.when(col.isNull(), F.lit(None))
        .when(str_len <= (keep_prefix + keep_suffix), mask_repeated)
        .otherwise(F.concat(prefix, mask_repeated, suffix))
    )


def tokenize_id(col: Column, salt: str | None = None) -> Column:
    """
    Cryptographic pseudonymization / tokenization using salted SHA-256 hash.
    Preserves referential joinability without exposing the raw natural key.

    Note: for low-cardinality, sequential keys (e.g. sk_id_curr) a plain hash is
    brute-forceable; prefer a keyed HMAC or a random surrogate mapping when the
    identifier space is small. The salt is read from PII_TOKENIZATION_SALT.
    """
    resolved_salt = salt or DEFAULT_SALT
    return (
        F.when(col.isNull(), F.lit(None))
        .otherwise(F.sha2(F.concat(col.cast(StringType()), F.lit(resolved_salt)), 256))
    )


def mask_income_bracket(col: Column) -> Column:
    """
    Generalizes numerical income into coarse categories (k-anonymity / binning).
    Protects exact customer wealth while retaining demographic risk segmentation.
    """
    val = col.cast("double")
    return (
        F.when(val.isNull(), F.lit("Unknown"))
        .when(val < 50000, F.lit("<50k"))
        .when(val < 100000, F.lit("50k-100k"))
        .when(val < 200000, F.lit("100k-200k"))
        .when(val < 500000, F.lit("200k-500k"))
        .otherwise(F.lit(">=500k"))
    )
