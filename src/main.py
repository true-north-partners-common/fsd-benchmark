import polars as pl

from datetime import date
from pathlib import Path

from src.hpi import fetch_hpi_data
from src.pp import fetch_pp_data
from src.postcode_lookup import fetch_postcode_lookup
from src.calculate_fsd import merge_and_process_data

DESTINATION_PATH = Path(__file__).parent.parent / "data"
PP_URL_TEMPCHUNK_SIZE = 1024 * 1024
PROGRESS_BAR_WIDTH = 40
START_DATE = date(1995, 1, 1)
END_DATE = date.today()


def main() -> pl.LazyFrame:
    """Main ETL pipeline function to fetch and process PP and HPI data.

    Returns:
        pl.DataFrame: The final processed FSD summary.
    """
    pp_lf = fetch_pp_data(destination=DESTINATION_PATH / "pp.csv")
    hpi_lf = fetch_hpi_data(
        start_date=date(1995, 1, 1),
        end_date=date.today(),
        destination=DESTINATION_PATH / "hpi.parquet",
    )
    postcode_lookup_lf = fetch_postcode_lookup(
        destination=DESTINATION_PATH / "postcode_lookup.parquet"
    )
    pp_lf, fsd_summary = merge_and_process_data(
        pp_lf, hpi_lf, postcode_lookup_lf
    )
    return fsd_summary

if __name__ == "__main__":
    df = main()
    print(df)
