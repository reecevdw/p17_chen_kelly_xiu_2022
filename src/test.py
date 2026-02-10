import pandas as pd
from pathlib import Path
from settings import config

DATA_DIR = Path(config("DATA_DIR"))
xw = pd.read_parquet(DATA_DIR / "raven_crsp_crosswalk.parquet")

print("permnos per rp_entity_id:")
print(xw.groupby("rp_entity_id")["permno"].nunique().value_counts().head(10))

print("rp_entity_ids per permno:")
print(xw.groupby("permno")["rp_entity_id"].nunique().value_counts().head(10))