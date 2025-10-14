"""Price paid data loading.

Data is loaded from the data directory which contains the zip files downloaded from
https://github.com/dmaso01/ppd?tab=readme-ov-file
"""

import polars as pl
import sys

from datetime import date
from urllib.request import Request, urlopen
from pathlib import Path

URL = (
    "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.csv"
)


def _render_progress(downloaded: int, total_bytes: int | None, progress_bar_width: int = 40) -> str:
    """Build an inline progress bar string for the current download status.

    Args:
        downloaded (int): Number of bytes that have been written locally.
        total_bytes (int | None): total_bytes number of bytes expected according to
            ``Content-Length``.
        progress_bar_width (int): Width of the progress bar in characters.

    Returns:
        str: Human-readable progress line suitable for terminal output.
    """
    if total_bytes and total_bytes > 0:
        fraction = min(downloaded / total_bytes, 1)
        filled = int(progress_bar_width * fraction)
        bar = "#" * filled + "-" * (progress_bar_width - filled)
        percent = fraction * 100
        return (
            f"[{bar}] {percent:5.1f}% "
            f"({downloaded / (1024**2):.2f} MiB/{total_bytes / (1024**2):.2f} MiB)"
        )
    return f"Downloaded {downloaded / (1024**2):.2f} MiB"


def download_csv(destination: Path, chunk_size: int = 1024 * 1024) -> None:
    """Download a CSV file from ``url`` into ``destination`` with progress output.

    Args:
        destination (Path): Filesystem location to store the downloaded CSV.
        chunk_size (int): Number of bytes to read from the response at a time.

    Raises:
        RuntimeError: If the HTTP response status is not 200.
    """
    if destination.exists():
        print("Price paid data already downloaded! Skipping download step...")
        return

    destination.parent.mkdir(parents=True, exist_ok=True)

    request = Request(URL, headers={"User-Agent": "python-urllib/3"})
    with urlopen(request) as response:  # noqa: S310 - trusted publicdata source
        status = getattr(response, "status", response.getcode())
        if status != 200:
            raise RuntimeError(f"Request failed with status {status}")

        content_length_header = response.headers.get("Content-Length")
        total_bytes = int(content_length_header) if content_length_header else None

        downloaded = 0
        with destination.open("wb") as handle:
            while True:
                chunk = response.read(chunk_size)
                if not chunk:
                    break
                handle.write(chunk)
                downloaded += len(chunk)
                sys.stdout.write(
                    "\r" + _render_progress(downloaded=downloaded, total_bytes=total_bytes)
                )
                sys.stdout.flush()

    sys.stdout.write("\n")


def load_data(path: Path) -> pl.LazyFrame:
    schema_overrides = {
        "postcode": pl.Utf8,
        "property_type": pl.Utf8,
        "tenure": pl.Utf8,
        "paon": pl.Utf8,
        "saon": pl.Utf8,
        "street": pl.Utf8,
        "locality": pl.Utf8,
        "district": pl.Utf8,
        "county": pl.Utf8,
        "ppd_category": pl.Utf8,
        "record_status": pl.Utf8,
        "price": pl.Int64,
    }
    return pl.scan_csv(
        path,
        has_header=False,
        new_columns=[
            "transaction_id",
            "price",
            "transfer_date",
            "postcode",
            "property_type",
            "new_build",
            "tenure",
            "paon",
            "saon",
            "street",
            "locality",
            "town_city",
            "district",
            "county",
            "ppd_category",
            "record_status",
        ],
        schema_overrides=schema_overrides,
    )


def massage_data(lf: pl.LazyFrame) -> pl.LazyFrame:
    lf = lf.with_columns(
        pl.col("transaction_id").str.strip_prefix("{").str.strip_suffix("}").name.keep(),
        pl.col(
            "postcode",
            "property_type",
            "tenure",
            "paon",
            "saon",
            "street",
            "locality",
            "district",
            "county",
            "ppd_category",
            "record_status",
        )
        .str.to_lowercase()
        .name.keep()
    )

    verbose_property_type_map = {
        "d": "detached",
        "s": "semi-detached",
        "t": "terraced",
        "f": "flat/maisonette",
        "o": "other",
    }
    lf = lf.with_columns(
        pl.col("property_type").replace(verbose_property_type_map, default=None).name.keep()
    )

    lf = lf.with_columns(
        pl.col("new_build").str.to_lowercase().eq("y").name.keep(),
        pl.col("transfer_date")
        .str.split(" ")
        .list.get(0)
        .str.to_date(format="%Y-%m-%d")
        .name.keep(),
    )

    lf = lf.with_columns(
        pl.concat_str(
            "paon",
            "saon",
            "street",
            "locality",
            "town_city",
            "district",
            "postcode",
            separator=", ",
        )
        .str.to_lowercase()
        .alias("full_address"),
    )

    lf = lf.sort("full_address", "transfer_date")

    lf = lf.with_columns(
        pl.col("transfer_date", "ppd_category", "price")
        .shift(1)
        .over("full_address")
        .name.suffix("_lag"),
        pl.col("ppd_category").shift(-1).over("full_address").name.suffix("_lead"),
        pl.col("full_address").count().over("full_address").alias("no_of_times_sold"),
    )
    lf = lf.with_columns(
        pl.when(pl.col("transfer_date_lag").is_null())
        .then(pl.lit(None))
        .otherwise(
            pl.date_ranges(
                pl.col("transfer_date_lag"), pl.col("transfer_date"), "1mo", closed="right"
            ).list.len()
        )
        .alias("months_since_last_sale")
    )
    lf = lf.filter(
        (pl.col("ppd_category").eq("b") & pl.col("ppd_category_lag").eq("a"))
        | (pl.col("ppd_category").eq("a") & pl.col("ppd_category_lead").eq("b"))
    )
    lf = lf.filter(pl.col("price").gt(10000))
    lf = lf.filter(pl.col("postcode").is_not_null())
    lf = lf.filter(
        pl.col("ppd_category").eq("a")
        | (pl.col("ppd_category").eq("b") & pl.col("transfer_date").ge(date(2013, 10, 1)))
    )
    lf = lf.filter(pl.col("property_type").ne("other"))
    lf = lf.filter(pl.col("no_of_times_sold").gt(1))
    lf = lf.filter(
        pl.col("ppd_category").eq("a")
        | (pl.col("months_since_last_sale").gt(12) & pl.col("ppd_category").eq("b"))
    )
    return lf


def fetch_pp_data(destination: Path) -> pl.LazyFrame:
    """Download the price paid data from ``url`` into ``destination`` and load it.

    Args:
        destination (Path): Filesystem location to store the downloaded CSV.

    Returns:
        pl.LazyFrame: Lazy frame containing the price paid data.
    """
    download_csv(destination=destination)
    lf = load_data(path=destination)
    lf = massage_data(lf=lf)
    return lf

