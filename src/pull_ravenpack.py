"""
Functions to pull RavenPack Dow Jones news data from WRDS for
recreating Chen, Kelly, and Xiu (2022).

 - RavenPack DJ Precision Equities: ravenpack_dj.rpa_djpr_equities_YYYY
 - Tables available from 2000 to 2019
 - WRDS docs: https://wrds-www.wharton.upenn.edu/pages/get-data/ravenpack/

Filters applied following the paper's methodology:
 - country_code = 'US'
 - relevance = 100 (highest relevance to the entity)
 - Single-stock stories only (1 distinct rp_entity_id per rp_story_id)
 - Deduplicated to one row per rp_story_id (RavenPack assigns multiple
   events/topics per story, producing duplicate headline rows)

"""

from pathlib import Path

import pandas as pd
import wrds

from settings import config

DATA_DIR = Path(config("DATA_DIR"))
WRDS_USERNAME = config("WRDS_USERNAME")

# Paper timeframe: RavenPack DJ tables cover 2000-2019
START_YEAR = 2000
END_YEAR = 2019
END_DATE = "2019-06-30"


def pull_ravenpack_news(
    start_year=START_YEAR, end_year=END_YEAR, wrds_username=WRDS_USERNAME
):
    """
    Pull US single-stock news headlines from RavenPack DJ edition.

    Loops through yearly tables (ravenpack_dj.rpa_djpr_equities_YYYY)
    using a single WRDS connection. Applies relevance>=90 and US filters
    in SQL, then single-stock and deduplication filters in pandas.
    """
    db = wrds.Connection(wrds_username=wrds_username)

    dfs = []
    for year in range(start_year, end_year + 1):
        print(f"Pulling RavenPack data for {year}...")
        table = f"rpa_djpr_equities_{year}"

        query = f"""
        SELECT
            timestamp_utc,
            rp_story_id,
            rp_entity_id,
            entity_name,
            headline,
            news_type,
            event_sentiment_score
        FROM ravenpack_dj.{table}
        WHERE
            country_code = 'US'
            AND relevance >= 90
        """

        df = db.raw_sql(query, date_cols=["timestamp_utc"])
        dfs.append(df)
        print(f"  {year}: {len(df):,} rows")

    db.close()

    df_all = pd.concat(dfs, ignore_index=True)
    print(f"Raw total: {len(df_all):,} rows")

    # Trim to paper end date (include all of June 30)
    df_all = df_all[df_all["timestamp_utc"] < pd.Timestamp("2019-07-01")]
    print(f"After end-date filter ({END_DATE}): {len(df_all):,} rows")

    # Single-stock filter: keep stories about exactly one entity
    entity_counts = df_all.groupby("rp_story_id")["rp_entity_id"].nunique()
    single_stock = entity_counts[entity_counts == 1].index
    before = len(df_all)
    df_all = df_all[df_all["rp_story_id"].isin(single_stock)]
    print(f"Single-stock filter: {before:,} -> {len(df_all):,} rows")

    # Deduplicate: keep one row per story
    before = len(df_all)
    df_all = df_all.drop_duplicates(subset=["rp_story_id"])
    print(f"Deduplicated: {before:,} -> {len(df_all):,} rows")

    return df_all


def load_ravenpack_news(data_dir=DATA_DIR):
    """Load saved RavenPack news data from parquet file."""
    path = Path(data_dir) / "RAVENPACK_NEWS.parquet"
    return pd.read_parquet(path)


if __name__ == "__main__":
    df = pull_ravenpack_news()

    path = DATA_DIR / "RAVENPACK_NEWS.parquet"
    df.to_parquet(path, index=False)
    print(f"Saved {len(df):,} rows to {path}")
