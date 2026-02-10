"""
Functions to pull daily CRSP stock data and construct a Russell 1000
proxy universe for recreating Chen, Kelly, and Xiu (2022).

 - CRSP Daily Stock File: https://wrds-www.wharton.upenn.edu/data-dictionary/crsp_a_stock/dsf/
 - CRSP Name History: https://wrds-www.wharton.upenn.edu/data-dictionary/crsp_a_stock/msenames/
 - Delisting returns: Bali, Engle, Murray (2016), Ch. 7
 - CRSP adjustment factors: https://vimeo.com/443061703
 - Useful reference: https://www.tidy-finance.org/python/wrds-crsp-and-compustat.html

Paper timeframe: Jan 1996 to June 2019. Start date is pulled one month
early (Dec 1995) to allow for lagged calculations (e.g., t-1 returns
for 3-day cumulative return labels).

"""

from pathlib import Path

import numpy as np
import pandas as pd
import wrds

from settings import config

DATA_DIR = Path(config("DATA_DIR"))
WRDS_USERNAME = config("WRDS_USERNAME")

# Paper timeframe: Jan 1996 to June 2019
# NOTE: since ravenpack goes back to 2000, we need dont need to pull CRSP data before 2000.
START_DATE = "2000-01-01"
END_DATE = "2019-06-30"


def pull_CRSP_daily_file(
    start_date=START_DATE, end_date=END_DATE, wrds_username=WRDS_USERNAME
):
    """
    Pulls DAILY CRSP stock data with robust market cap calculations.
    """

    query = f"""
    SELECT
        dsf.date,
        dsf.permno, dsf.permco,
        dsf.cusip,
        dsf.ret, dsf.retx,
        dsf.prc, dsf.openprc,
        dsf.vol, dsf.shrout,
        dsf.cfacshr, dsf.cfacpr,
        msedelist.dlret, msedelist.dlretx, msedelist.dlstcd
    FROM crsp.dsf AS dsf
    LEFT JOIN
        crsp.msenames as msenames
    ON
        dsf.permno = msenames.permno AND
        msenames.namedt <= dsf.date AND
        dsf.date <= msenames.nameendt
    LEFT JOIN
        crsp.msedelist as msedelist
    ON
        dsf.permno = msedelist.permno AND
        date_trunc('month', dsf.date)::date =
        date_trunc('month', msedelist.dlstdt)::date
    WHERE
        dsf.date BETWEEN '{start_date}' AND '{end_date}' AND
        msenames.shrcd IN (10, 11) -- Ordinary Common Shares
    """

    db = wrds.Connection(wrds_username=wrds_username)
    print("Executing query... this may take time due to daily data volume.")
    df = db.raw_sql(query, date_cols=["date"])
    db.close()

    # Drop duplicate rows from overlapping msenames date ranges
    df = df.loc[:, ~df.columns.duplicated()]
    df = df.drop_duplicates(subset=["permno", "date"])

    # --- Preprocessing ---

    # 1. Handle Prices (Negative means average of Bid/Ask)
    df["prc"] = df["prc"].abs()
    df["openprc"] = df["openprc"].abs()

    # 2. Handle Shares (CRSP shrout is in thousands)
    df["shrout"] = df["shrout"] * 1000

    # 3. Calculate "Adjusted" metrics (Per your snippet)
    # This aligns prices/shares to a base period, handling splits AND complex corporate actions
    # where cfacshr != cfacpr (like spinoffs).
    df["adj_shrout"] = df["shrout"] * df["cfacshr"]
    df["adj_prc"] = df["prc"] / df["cfacpr"]
    df["adj_openprc"] = df["openprc"] / df["cfacpr"]

    # 4. Calculate Market Cap
    # OPTION A (Nominal): Best for recreating Index Constituents (Russell 1000)
    # Uses the actual size of the company on that day.
    df["market_cap"] = df["prc"] * df["shrout"]

    # OPTION B (Adjusted): Best for time-series valuation analysis
    # Preserves value across spinoffs.
    df["market_cap_adj"] = df["adj_prc"] * df["adj_shrout"]

    # 5. Handle Delisting Returns
    df = apply_delisting_returns(df)

    return df


def apply_delisting_returns(df):
    """
    Use instructions for handling delisting returns from: Chapter 7 of
    Bali, Engle, Murray --
    Empirical Asset Pricing: The Cross Section of Stock Returns (2016)

    If dlret is NA and dlstcd is not NA, then:
    if dlstcd is 500, 520, 551-574, 580, or 584, then dlret = -0.3
    if dlret is NA but dlstcd is not one of the above, then dlret = -1
    """
    performance_codes = [500, 520, 580, 584] + list(range(551, 575))

    df["dlret"] = np.select(
        [
            df["dlstcd"].isin(performance_codes) & df["dlret"].isna(),
            df["dlret"].isna() & df["dlstcd"].notna() & (df["dlstcd"] >= 200),
        ],
        [-0.3, -1],
        default=df["dlret"],
    )

    df["dlretx"] = np.select(
        [
            df["dlstcd"].isin(performance_codes) & df["dlretx"].isna(),
            df["dlretx"].isna() & df["dlstcd"].notna() & (df["dlstcd"] >= 200),
        ],
        [-0.3, -1],
        default=df["dlretx"],
    )

    # Fill missing returns with delisting returns (only where dlret exists)
    df["ret"] = df["ret"].fillna(df["dlret"])
    df["retx"] = df["retx"].fillna(df["dlretx"])

    return df


def load_CRSP_daily_file(data_dir=DATA_DIR):
    """Load saved daily CRSP stock data from parquet file."""
    path = Path(data_dir) / "CRSP_DAILY_PAPER_UNIVERSE.parquet"
    df = pd.read_parquet(path)
    return df


def get_russell_1000_proxy(df):
    """
    Selects Top 1000 stocks by NOMINAL Market Cap.
    """
    print("Calculating Russell 1000 proxy...")

    # We use 'market_cap' (nominal) here because index membership
    # is based on how big the company actually IS today.
    df["rank"] = df.groupby("date")["market_cap"].rank(ascending=False)

    df_russell = df[df["rank"] <= 1000].copy()

    return df_russell


if __name__ == "__main__":
    df_daily = pull_CRSP_daily_file()
    df_universe = get_russell_1000_proxy(df_daily)

    path = Path(DATA_DIR) / "CRSP_DAILY_PAPER_UNIVERSE.parquet"
    df_universe.to_parquet(path)
    print(f"Saved {len(df_universe)} rows to {path}")
