import pandas as pd
import polars as pl

from pathlib import Path

URL = "https://www.roblocher.com/technotes/uk-postareas.html"


def fetch_postcode_lookup(destination: Path) -> pl.LazyFrame:
    """Fetch and process the postcode lookup data.

    Args:
        destination (Path): Path to save the downloaded data into a parquet file.

    Returns:
        pl.LazyFrame: Processed postcode lookup data.
    """
    if destination.exists():
        print("Postcode lookup data already downloaded! Skipping download step ...")
        return pl.read_parquet(destination).lazy()

    df = pd.read_html(URL)[0]
    df = pl.from_pandas(df)
    df = df.select(pl.all().str.to_lowercase().name.map(lambda col: col.lower().replace(" ", "_")))
    df = df.drop("postcode_area_name")
    df.write_parquet(destination)
    return df.lazy()

