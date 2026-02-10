"""
Generate simple exploratory charts to verify data pulls.

Outputs interactive HTML charts to _output/.
These are sanity-check plots, not final analysis.
"""

from pathlib import Path

import pandas as pd
import plotly.express as px

from settings import config

DATA_DIR = Path(config("DATA_DIR"))
OUTPUT_DIR = Path(config("OUTPUT_DIR"))

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ------------------------------------------------------------
# CRSP: Average market cap over time
# ------------------------------------------------------------
def chart_crsp_market_cap():
    path = DATA_DIR / "CRSP_DAILY_PAPER_UNIVERSE.parquet"
    df = pd.read_parquet(path, columns=["date", "market_cap"])

    daily = (
        df.groupby("date", as_index=False)["market_cap"]
        .mean()
        .rename(columns={"market_cap": "avg_market_cap"})
    )

    fig = px.line(
        daily,
        x="date",
        y="avg_market_cap",
        title="CRSP: Average Market Capitalization (Daily)",
    )

    out = OUTPUT_DIR / "crsp_avg_market_cap.html"
    fig.write_html(out)
    print(f"Saved {out}")


# ------------------------------------------------------------
# RavenPack: Daily article counts
# ------------------------------------------------------------
def chart_ravenpack_volume():
    path = DATA_DIR / "RAVENPACK_NEWS.parquet"
    df = pd.read_parquet(path, columns=["timestamp_utc"])

    df["date"] = pd.to_datetime(df["timestamp_utc"]).dt.normalize()
    daily = (
        df.groupby("date")
        .size()
        .reset_index(name="article_count")
    )

    fig = px.line(
        daily,
        x="date",
        y="article_count",
        title="RavenPack: Daily Article Volume (US, Single-Stock)",
    )

    out = OUTPUT_DIR / "ravenpack_daily_volume.html"
    fig.write_html(out)
    print(f"Saved {out}")


# ------------------------------------------------------------
# RavenPack x CRSP: Event sentiment distribution
# ------------------------------------------------------------
def chart_sentiment_distribution():
    path = DATA_DIR / "RAVENPACK_CRSP_MERGED.parquet"
    df = pd.read_parquet(path, columns=["event_sentiment_score"])

    df = df[df["event_sentiment_score"].notna()]

    # SAMPLE to keep HTML size reasonable
    if len(df) > 2_000_000:
        df = df.sample(2_000_000, random_state=0)

    fig = px.histogram(
        df,
        x="event_sentiment_score",
        nbins=100,
        title="RavenPack Event Sentiment Score Distribution (Sampled)",
    )

    out = OUTPUT_DIR / "ravenpack_sentiment_distribution.html"
    fig.write_html(out, include_plotlyjs="cdn")  # also reduces file size
    print(f"Saved {out}")


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------
if __name__ == "__main__":
    chart_crsp_market_cap()
    chart_ravenpack_volume()
    chart_sentiment_distribution()