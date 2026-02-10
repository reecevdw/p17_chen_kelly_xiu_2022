from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd
import wrds

from settings import config

DATA_DIR = Path(config("DATA_DIR"))
WRDS_USERNAME = config("WRDS_USERNAME")


def build_raven_crsp_crosswalk(out_path: Optional[Path] = None) -> Path:
    """
    Implements the WRDS SAS approach:

      permno from crsp.dse (uses historical ncusip, 8 chars)
      rp_entity_id + isin from rpna.wrds_rpa_company_names
      match: a.ncusip = substr(b.isin,3,8)

    Output: unique (permno, rp_entity_id)
    """
    if out_path is None:
        out_path = DATA_DIR / "raven_crsp_crosswalk.parquet"

    sql = """
    SELECT DISTINCT
        a.permno,
        b.rp_entity_id
    FROM crsp.dse AS a
    JOIN rpna.wrds_rpa_company_names AS b
      ON a.ncusip = SUBSTRING(b.isin FROM 3 FOR 8)
    WHERE a.ncusip IS NOT NULL
      AND a.ncusip <> ''
      AND b.isin IS NOT NULL
      AND b.isin <> ''
      AND b.rp_entity_id IS NOT NULL
      AND b.rp_entity_id <> ''
    ;
    """

    db = wrds.Connection(wrds_username=WRDS_USERNAME)
    try:
        xw = db.raw_sql(sql)
    finally:
        db.close()

    # Safety: make sure we don't duplicate on rp_entity_id in later merge
    xw = xw.drop_duplicates(subset=["permno", "rp_entity_id"]).reset_index(drop=True)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    xw.to_parquet(out_path, index=False)
    print(f"Saved {len(xw):,} links -> {out_path}")
    return out_path


def attach_permno_to_ravenpack(
    ravenpack_path: Optional[Path] = None,
    crosswalk_path: Optional[Path] = None,
    out_path: Optional[Path] = None,
) -> Path:
    """
    Left-join permno onto RavenPack news using rp_entity_id.
    """
    if ravenpack_path is None:
        ravenpack_path = DATA_DIR / "ravenpack_djpr.parquet"
    if crosswalk_path is None:
        crosswalk_path = DATA_DIR / "raven_crsp_crosswalk.parquet"
    if out_path is None:
        out_path = DATA_DIR / "ravenpack_djpr_with_permno.parquet"

    rp = pd.read_parquet(ravenpack_path)
    xw = pd.read_parquet(crosswalk_path)

    # Ensure crosswalk is unique by rp_entity_id to avoid row explosion.
    # If an rp_entity_id maps to multiple permnos (rare, but possible),
    # you can keep all by removing this line — but then merges can blow up.
    xw1 = xw.drop_duplicates(subset=["rp_entity_id"]).copy()

    rp2 = rp.merge(xw1, on="rp_entity_id", how="left")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    rp2.to_parquet(out_path, index=False)

    print(f"Saved RavenPack with permno -> {out_path}")
    print(f"Share matched to permno: {rp2['permno'].notna().mean():.3f}")
    return out_path


def merge_ravenpack_with_crsp_daily(
    ravenpack_with_permno_path: Optional[Path] = None,
    crsp_daily_path: Optional[Path] = None,
    out_path: Optional[Path] = None,
    how: str = "left",
) -> Path:
    """
    Merge RavenPack (now with permno) to CRSP daily file on (permno, date).

    how="left" keeps all RavenPack rows and brings CRSP fields when available (recommended).
    how="inner" keeps only rows that match CRSP daily (stricter).
    """
    if ravenpack_with_permno_path is None:
        ravenpack_with_permno_path = DATA_DIR / "ravenpack_djpr_with_permno.parquet"
    if crsp_daily_path is None:
        crsp_daily_path = DATA_DIR / "CRSP_DAILY_PAPER_UNIVERSE.parquet"
    if out_path is None:
        out_path = DATA_DIR / "ravenpack_crsp_merged.parquet"

    if how not in {"left", "inner"}:
        raise ValueError("how must be 'left' or 'inner'")

    rp = pd.read_parquet(ravenpack_with_permno_path)
    crsp = pd.read_parquet(crsp_daily_path)

    # RavenPack timestamp -> trading date key (normalize to midnight)
    rp = rp.copy()
    rp["date"] = pd.to_datetime(rp["timestamp_utc"]).dt.normalize()

    crsp = crsp.copy()
    crsp["date"] = pd.to_datetime(crsp["date"]).dt.normalize()

    merged = rp.merge(crsp, on=["permno", "date"], how=how)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_parquet(out_path, index=False)

    print(f"Saved merged RavenPack x CRSP ({how}) -> {out_path}")
    print(f"Rows: {len(merged):,}")
    return out_path


if __name__ == "__main__":
    build_raven_crsp_crosswalk()
    attach_permno_to_ravenpack()
    merge_ravenpack_with_crsp_daily(how="left")