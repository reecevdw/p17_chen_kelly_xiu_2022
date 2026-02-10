from __future__ import annotations

import time
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd
import pyarrow.parquet as pq
import wrds
from settings import config

DATA_DIR = Path(config("DATA_DIR"))
WRDS_USERNAME = config("WRDS_USERNAME")

# Project timeframe
START_DATE = "2000-01-01"
END_DATE = "2019-06-30"

SCHEMA = "ravenpack_dj"
TABLE_PREFIX = "rpa_djpr_equities_"

# Where to store intermediate yearly files
YEAR_DIR = DATA_DIR / "ravenpack_years"

# Quote reserved words for Postgres
FIELDS: List[str] = [
    "timestamp_utc",
    "rp_story_id",
    "rp_entity_id",
    "entity_type",
    "entity_name",
    "country_code",
    "relevance",
    "event_sentiment_score",
    "event_relevance",
    "event_similarity_key",
    "event_similarity_days",
    "topic",
    '"group"',  # reserved keyword
    '"type"',  # reserved keyword
    "sub_type",
    "property",
    "fact_level",
    "category",
    "news_type",
    "rp_source_id",
    "source_name",
    "provider_id",
    "provider_story_id",
    "headline",
    "css",
]


def year_range(start_date: str, end_date: str) -> List[int]:
    return list(range(int(start_date[:4]), int(end_date[:4]) + 1))


def year_bounds_for_project(year: int) -> Tuple[str, str]:
    """
    Year-specific bounds aligned to the overall project window.
    - For years strictly inside: Jan 1 to Dec 31
    - For 2019: Jan 1 to END_DATE (June 30)
    """
    if year == int(END_DATE[:4]):
        return (f"{year}-01-01", END_DATE)
    return (f"{year}-01-01", f"{year}-12-31")


def _year_file_path(year: int) -> Path:
    return YEAR_DIR / f"ravenpack_djpr_{year}.parquet"


def pull_ravenpack_single_firm_year(
    year: int,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: Optional[int] = None,
    event_only: bool = False,
) -> pd.DataFrame:
    """
    Pull RavenPack DJPR equities for one year, filtered to:
      - US companies, relevance>=90
      - single-firm stories (COUNT DISTINCT rp_entity_id == 1 per provider story)
      - optional event_only: event_sentiment_score IS NOT NULL

    Set limit=None to pull all.
    """
    if start_date is None or end_date is None:
        start_date, end_date = year_bounds_for_project(year)

    table = f"{TABLE_PREFIX}{year}"
    select_cols = ", ".join([f"t.{c}" for c in FIELDS])

    event_filter_cte = "AND event_sentiment_score IS NOT NULL" if event_only else ""
    event_filter_outer = "AND t.event_sentiment_score IS NOT NULL" if event_only else ""
    limit_sql = f"LIMIT {int(limit)}" if limit is not None else ""

    sql = f"""
    WITH single_firm AS (
        SELECT
            provider_id,
            provider_story_id
        FROM {SCHEMA}.{table}
        WHERE entity_type = 'COMP'
          AND country_code = 'US'
          AND relevance >= 90
          AND timestamp_utc >= '{start_date}'
          AND timestamp_utc <  '{end_date}'::date + interval '1 day'
          {event_filter_cte}
        GROUP BY provider_id, provider_story_id
        HAVING COUNT(DISTINCT rp_entity_id) = 1
    )
    SELECT {select_cols}
    FROM {SCHEMA}.{table} t
    JOIN single_firm s
      ON t.provider_id = s.provider_id
     AND t.provider_story_id = s.provider_story_id
    WHERE t.entity_type = 'COMP'
      AND t.country_code = 'US'
      AND t.relevance >= 90
      AND t.timestamp_utc >= '{start_date}'
      AND t.timestamp_utc <  '{end_date}'::date + interval '1 day'
      {event_filter_outer}
    {limit_sql}
    ;
    """

    db = wrds.Connection(wrds_username=WRDS_USERNAME)
    try:
        df = db.raw_sql(sql, date_cols=["timestamp_utc"])
    finally:
        db.close()

    # Rename awkward column names (from quoted SQL identifiers)
    df = df.rename(columns={"group": "rp_group", "type": "rp_type"})
    return df


def pull_missing_years_to_parquet(
    event_only: bool = False,
    limit: Optional[int] = None,
    force: bool = False,
    max_retries: int = 3,
    retry_sleep_seconds: int = 10,
) -> List[Path]:
    """
    Pull each year into YEAR_DIR, skipping years already present unless force=True.
    Retries failed years up to max_retries.
    """
    YEAR_DIR.mkdir(parents=True, exist_ok=True)

    years = year_range(START_DATE, END_DATE)
    saved: List[Path] = []

    for y in years:
        out_y = _year_file_path(y)

        if out_y.exists() and not force:
            print(f"Skipping {y} (already exists): {out_y}")
            saved.append(out_y)
            continue

        y_start, y_end = year_bounds_for_project(y)

        attempt = 0
        while True:
            attempt += 1
            try:
                print(f"Pulling RavenPack {y} ({y_start} to {y_end}) [attempt {attempt}] ...")
                df_y = pull_ravenpack_single_firm_year(
                    year=y,
                    start_date=y_start,
                    end_date=y_end,
                    limit=limit,            # None = full year
                    event_only=event_only,  # False = true superset
                )

                df_y["year"] = y
                df_y.to_parquet(out_y, index=False)
                print(f"  saved {len(df_y):,} rows -> {out_y}")
                saved.append(out_y)
                break

            except Exception as e:
                print(f"  ERROR pulling {y}: {e}")
                if attempt >= max_retries:
                    raise
                print(f"  retrying in {retry_sleep_seconds}s ...")
                time.sleep(retry_sleep_seconds)

    return saved


def combine_year_parquets_to_single(
    out_path: Path | None = None,
    year_files: Optional[List[Path]] = None,
) -> Path:
    """
    Combine yearly parquet files into ONE parquet file efficiently using pyarrow ParquetWriter.
    This avoids loading everything into memory.
    """
    if out_path is None:
        out_path = DATA_DIR / "ravenpack_djpr.parquet"

    if year_files is None:
        year_files = sorted(YEAR_DIR.glob("ravenpack_djpr_*.parquet"))

    if not year_files:
        raise FileNotFoundError(f"No yearly parquet files found in {YEAR_DIR}")

    writer: pq.ParquetWriter | None = None
    try:
        for p in year_files:
            table = pq.read_table(p)
            if writer is None:
                writer = pq.ParquetWriter(out_path, table.schema, compression="snappy")
            writer.write_table(table)
            print(f"Appended {p.name} ({table.num_rows:,} rows)")
    finally:
        if writer is not None:
            writer.close()

    print(f"Wrote combined parquet -> {out_path}")
    return out_path


def save_ravenpack_parquet(
    out_path: Path | None = None,
    event_only: bool = False,
    limit: Optional[int] = None,
    force: bool = False,
    max_retries: int = 3,
    retry_sleep_seconds: int = 10,
) -> Path:
    """
    Your requested workflow:
      1) Pull missing years into _data/ravenpack_years/
      2) Combine those into ONE parquet file at _data/ravenpack_djpr.parquet
    """
    year_files = pull_missing_years_to_parquet(
        event_only=event_only,
        limit=limit,
        force=force,
        max_retries=max_retries,
        retry_sleep_seconds=retry_sleep_seconds,
    )
    return combine_year_parquets_to_single(out_path=out_path, year_files=year_files)


if __name__ == "__main__":
    save_ravenpack_parquet(
        out_path=DATA_DIR / "ravenpack_djpr.parquet",
        event_only=False,  # superset
        limit=None,        # set to e.g. 10000 for a test run
        force=False,       # only pull missing years
        max_retries=3,
        retry_sleep_seconds=10,
    )