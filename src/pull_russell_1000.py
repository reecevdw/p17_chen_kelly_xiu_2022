"""
Functions to pull Russell 1000 constituents from iShares ETF holdings.

The Russell 1000 index represents the top 1000 US companies by market cap.
We use the iShares Russell 1000 ETF (IWB) holdings as a proxy for the index
constituents, as the actual index constituents are proprietary.

 - iShares IWB: https://www.ishares.com/us/products/239707/ishares-russell-1000-etf
 - Russell 1000 methodology: https://www.lseg.com/en/ftse-russell/indices/russell-us

"""

import io
from pathlib import Path

import pandas as pd
import requests

from settings import config

DATA_DIR = Path(config("DATA_DIR"))


def pull_russell_1000_constituents():
    """
    Pulls Russell 1000 constituents from iShares Russell 1000 ETF (IWB) holdings.

    Returns a DataFrame with ticker symbols and related holdings data.
    """
    # URL for the iShares Russell 1000 ETF (IWB) holdings
    url = "https://www.ishares.com/us/products/239707/ishares-russell-1000-etf/1467271812596.ajax?fileType=csv&fileName=IWB_holdings&dataType=fund"

    response = requests.get(url)
    response.raise_for_status()

    # The CSV usually has metadata at the top. We need to find where the header starts.
    # iShares CSVs often start the actual data around line 10.
    csv_data = response.content.decode("utf-8")

    # Load into pandas (skip metadata rows)
    df = pd.read_csv(io.StringIO(csv_data), skiprows=9)

    # Clean up: Ensure we have the 'Ticker' column and remove non-equity rows (like cash)
    if "Ticker" not in df.columns:
        raise ValueError("'Ticker' column not found in the CSV.")

    # Filter out rows where Ticker is NaN or special formatting
    df = df.dropna(subset=["Ticker"])

    # iShares sometimes includes cash/futures with weird tickers; filter for valid strings
    df = df[df["Ticker"].apply(lambda t: isinstance(t, str) and t.isalpha())]

    # Drop all non-equity holdings (like cash, futures, etc.) based on the 'Asset Class' column
    if "Asset Class" in df.columns:
        df = df[df["Asset Class"].str.contains("Equity", case=False, na=False)]

    return df


def load_russell_1000_constituents(data_dir=DATA_DIR):
    """Load saved Russell 1000 constituents from parquet file."""
    path = Path(data_dir) / "RUSSELL_1000_CONSTITUENTS.parquet"
    df = pd.read_parquet(path)
    return df


def _demo():
    df = load_russell_1000_constituents(data_dir=DATA_DIR)
    print(f"Loaded {len(df)} Russell 1000 constituents.")
    print(df.head())


if __name__ == "__main__":
    df = pull_russell_1000_constituents()
    print(f"Successfully retrieved {len(df)} tickers.")
    print(f"First 10 tickers: {df['Ticker'].head(10).tolist()}")

    path = Path(DATA_DIR) / "RUSSELL_1000_CONSTITUENTS.parquet"
    df.to_parquet(path)
    print(f"Saved to {path}")
