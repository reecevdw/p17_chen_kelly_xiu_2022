# This pulls all to RAM then saves. 

from __future__ import annotations

from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd
import wrds
from settings import config

DATA_DIR = Path(config("DATA_DIR"))
WRDS_USERNAME = config("WRDS_USERNAME")

# Project timeframe
START_DATE = "2000-01-01"
END_DATE = "2019-06-30"

SCHEMA = "ravenpack_dj"
TABLE_PREFIX = "rpa_djpr_equities_"

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
    '"type"',   # reserved keyword
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
    - For 2000: Jan 1 to Dec 31 (fine)
    - For 2019: Jan 1 to END_DATE (June 30)
    """
    if year == int(END_DATE[:4]):
        return (f"{year}-01-01", END_DATE)
    return (f"{year}-01-01", f"{year}-12-31")


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

    # Rename awkward column names
    df = df.rename(columns={"group": "rp_group", "type": "rp_type"})
    return df


def save_ravenpack_parquet(
    out_path: Path | None = None,
    event_only: bool = False,
    limit: Optional[int] = None,
) -> Path:
    """
    Pull RavenPack superset across all years and save as ONE parquet file.
    """
    if out_path is None:
        out_path = DATA_DIR / "ravenpack_djpr.parquet"

    years = year_range(START_DATE, END_DATE)
    dfs: list[pd.DataFrame] = []

    for y in years:
        y_start, y_end = year_bounds_for_project(y)
        print(f"Pulling RavenPack {y} ({y_start} to {y_end}) ...")

        df_y = pull_ravenpack_single_firm_year(
            year=y,
            start_date=y_start,
            end_date=y_end,
            limit=limit,            # None = full year
            event_only=event_only,  # False = true superset
        )

        df_y["year"] = y  # key by year
        dfs.append(df_y)

        print(f"  pulled {len(df_y):,} rows")

    print("Concatenating all years ...")
    df_all = pd.concat(dfs, ignore_index=True)

    print(f"Writing {len(df_all):,} rows to {out_path}")
    df_all.to_parquet(out_path, index=False)

    return out_path

if __name__ == "__main__":
    save_ravenpack_parquet(
        event_only=False,
        limit=None,   # set to e.g. 10000 if you ever want a test run
    )