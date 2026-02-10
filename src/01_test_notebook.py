# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.19.1
#   kernelspec:
#     display_name: p17_chen_kelly_xiu_2022
#     language: python
#     name: python3
# ---

# %% [markdown]
# ## TEST NOTEBOOK

# %%
import wrds
import pandas as pd
from datetime import datetime
from pathlib import Path


import numpy as np
import pandas as pd
import wrds
from dateutil.relativedelta import relativedelta

from settings import config

DATA_DIR = Path(config("DATA_DIR"))
WRDS_USERNAME = config("WRDS_USERNAME")
START_DATE = config("START_DATE")
END_DATE = config("END_DATE")


# %%
with wrds.Connection(wrds_username=WRDS_USERNAME) as db:
    try:
        # List all available libraries (schemas)
        libraries = db.list_libraries()
        # print(libraries)

        # Filter for RavenPack specifically
        ravenpack_libs = [lib for lib in sorted(libraries) if "raven" in lib.lower()]
        print("RavenPack libraries:", ravenpack_libs)

        for lib in ravenpack_libs:
            print(f"Tables in {lib} library:")
            tables = db.list_tables(library=lib)
            print(tables)
    except Exception as e:
        print("An error occurred while connecting to WRDS or fetching data:", e)

# %% [markdown]
# ## RavenPack Libraries on WRDS
#
# ### 1. `ravenpack_common`
# Reference/mapping tables:
#
# | Table | Description |
# |-------|-------------|
# | `common_chars` | Common characteristics |
# | `rpa_company_mappings` | Company ID mappings |
# | `rpa_entity_mappings` | Entity ID mappings |
# | `rpa_source_list` | News source list |
# | `rpa_taxonomy` | Event taxonomy/categories |
# | `wrds_rpa_all_mappings` | WRDS combined mappings |
# | `wrds_rpa_company_mappings` | WRDS company mappings |
# | `wrds_rpa_company_names` | Company names |
# | `wrds_rpa_entity_mappings` | WRDS entity mappings |
# | `wrds_rpa_source_list` | WRDS source list |
#
# ### 2. `ravenpack_dj` (Dow Jones Edition)
#
# | Table Type | Years | Description |
# |------------|-------|-------------|
# | `djpr_chars` | - | Characteristics/metadata |
# | `rpa_djpr_equities_YYYY` | 2000-2025 | Equity news sentiment (26 tables) |
# | `rpa_djpr_global_macro_YYYY` | 2000-2025 | Macro news sentiment (26 tables) |
#
# ### 3. `ravenpack_trial`
#
# | Table | Description |
# |-------|-------------|
# | `chars` | Characteristics |
# | `rpa_entity_mappings` | Entity mappings |
# | `rpa_full_equities` | Full equities data (single table) |
# | `rpa_full_global_macro` | Full macro data (single table) |
# | `rpa_source_list` | Source list |
# | `rpa_taxonomy` | Event taxonomy |
# | `wrds_rpa_all_mappings` | Combined mappings |
# | `wrds_rpa_company_names` | Company names |
#
# **Note:** The trial library has consolidated tables (`rpa_full_*`) while the DJ edition splits data by year.

# %%
# with wrds.Connection(wrds_username=WRDS_USERNAME) as db:
#     # db.describe_table(library="ravenpack_trial", table="rpa_full_equities")
#     # df = db.get_table(library="ravenpack_trial", table="rpa_full_equities")
#     # print(df.head())
#     query = """
#     SELECT * FROM ravenpack_dj.rpa_djpr_equities_2000
#     WHERE country_code = 'US'
# """
#     df = db.raw_sql(query)


# filters = f"""country_code = 'US'
#     AND timestamp_utc >= '{START_DATE}'
#     AND timestamp_utc <= '{END_DATE}'"""
query = """
WITH SingleStockStories AS (
    SELECT
        rp_story_id
    FROM ravenpack_dj.rpa_djpr_equities_{YEAR}
    WHERE
        country_code = 'US'
        AND relevance = 100
    GROUP BY rp_story_id
    HAVING COUNT(DISTINCT rp_entity_id) = 1
)

SELECT
    *
FROM ravenpack_dj.rpa_djpr_equities_{YEAR} t
JOIN SingleStockStories s
    ON t.rp_story_id = s.rp_story_id
WHERE
    t.country_code = 'US'
    AND t.relevance = 100
"""

with wrds.Connection(wrds_username=WRDS_USERNAME) as db:
    df = db.raw_sql(query.format(YEAR=2000))


# df.head()


# %%
"""
Index(['rpa_date_utc', 'rpa_time_utc', 'timestamp_utc', 'rp_story_id',
       'rp_entity_id', 'entity_type', 'entity_name', 'country_code',
       'relevance', 'event_sentiment_score', 'event_relevance',
       'event_similarity_key', 'event_similarity_days', 'topic', 'group',
       'type', 'sub_type', 'property', 'fact_level', 'rp_position_id',
       'position_name', 'evaluation_method', 'maturity', 'earnings_type',
       'event_start_date_utc', 'event_end_date_utc', 'reporting_period',
       'reporting_start_date_utc', 'reporting_end_date_utc', 'related_entity',
       'relationship', 'category', 'event_text', 'news_type', 'rp_source_id',
       'source_name', 'css', 'nip', 'peq', 'bee', 'bmq', 'bam', 'bca', 'ber',
       'anl_chg', 'mcq', 'rp_story_event_index', 'rp_story_event_count',
       'product_key', 'provider_id', 'provider_story_id', 'headline'],
      dtype='object')
"""
with wrds.Connection(wrds_username=WRDS_USERNAME) as db:
    query = """
    WITH SingleStockStories AS (
        SELECT
            rp_story_id
        FROM ravenpack_dj.rpa_djpr_equities_2000
        WHERE
            country_code = 'US'
            AND relevance = 100
        GROUP BY rp_story_id
        HAVING COUNT(DISTINCT rp_entity_id) = 1
    )
    SELECT 
        t.rpa_date_utc,
        t.rpa_time_utc,
        t.timestamp_utc,
        t.rp_story_id,
        t.rp_entity_id,
        t.entity_name,
        t.headline,
        t.news_type,
        t.relevance
      FROM ravenpack_dj.rpa_djpr_equities_2000 t
    JOIN SingleStockStories s
        ON t.rp_story_id = s.rp_story_id
    WHERE 
        t.country_code = 'US'
        AND t.relevance = 100
    ORDER BY t.timestamp_utc ASC
"""
    df2 = db.raw_sql(query, date_cols=["rpa_date_utc", "rpa_time_utc", "timestamp_utc"])


df2.head()

# %%
with wrds.Connection(wrds_username=WRDS_USERNAME) as db:
    mappings = db.get_table(
        library="ravenpack_common", table="wrds_rpa_company_mappings"
    )


# %%
mappings

# %%
