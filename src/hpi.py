"""HPI data fetching."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any, Mapping, Sequence
from urllib.request import Request, urlopen

import polars as pl


URL_TEMPLATE = (
    "https://landregistry.data.gov.uk/app/ukhpi/download/new.json"
    "?from={start}&to={end}"
    "&location%5B%5D=K02000001&location%5B%5D=W92000004"
    "&location%5B%5D=S92000003&location%5B%5D=E92000001"
    "&location%6B%5D=N92000002&st%5B%5D=all&in%5B%5D=hpi"
)


def build_request_url(start_date: date, end_date: date) -> str:
    """Create a UK HPI download URL for the supplied date range.

    Args:
        start_date (date): Inclusive lower bound of the series to fetch.
        end_date (date): Inclusive upper bound of the series to fetch.

    Returns:
        str: Formatted request URL pointing to the UK HPI download endpoint.

    Raises:
        ValueError: When ``start_date`` occurs after ``end_date``.
    """
    if start_date > end_date:
        raise ValueError("start_date must be on or before end_date")

    return URL_TEMPLATE.format(
        start=start_date.isoformat(),
        end=end_date.isoformat(),
    )


def request_hpi_data(start_date: date, end_date: date) -> Any:
    """Retrieve the raw UK HPI JSON payload for the provided date range.

    Args:
        start_date (date): Inclusive lower bound of the series to fetch.
        end_date (date): Inclusive upper bound of the series to fetch.

    Returns:
        Any: Decoded JSON payload received from the UK HPI endpoint.

    Raises:
        RuntimeError: When the HTTP request does not succeed.
    """
    url = build_request_url(start_date=start_date, end_date=end_date)
    request = Request(url, headers={"User-Agent": "python-urllib/3"})
    with urlopen(request) as response:  # noqa: S310 - trusted data.gov.uk endpoint
        if response.status != 200:
            raise RuntimeError(f"Request failed with status {response.status}")
        return json.load(response)


def _first_record_list(payload: Any) -> Sequence[dict[str, Any]]:
    """Locate the first list of dictionary records within a payload.

    Args:
        payload (Any): JSON-like structure returned from the UK HPI endpoint.

    Returns:
        Sequence[dict[str, Any]]: First encountered list containing mapping records.

    Raises:
        ValueError: When no suitable list of records can be located.
    """
    if isinstance(payload, list):
        if payload and not isinstance(payload[0], dict):
            raise ValueError("Expected list of dicts in payload.")
        return payload

    if isinstance(payload, dict):
        for value in payload.values():
            if isinstance(value, list) and value and isinstance(value[0], dict):
                return value  # type: ignore[return-value]

    raise ValueError("Could not locate a list of records in the payload.")


def _flatten_record(record: Mapping[str, Any]) -> dict[str, Any]:
    """Expand nested mapping and single-item list values into scalar fields.

    Args:
        record (Mapping[str, Any]): Mapping representing a single row returned by
            the API.

    Returns:
        dict[str, Any]: Flattened dictionary with nested keys promoted using
        ``snake_case`` concatenation.
    """
    flat: dict[str, Any] = {}

    def _unpack(key: str, value: Any) -> None:
        if isinstance(value, Mapping):
            for inner_key, inner_value in value.items():
                composed_key = f"{key}_{inner_key}" if key else inner_key
                _unpack(composed_key, inner_value)
            return

        if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
            if len(value) == 1:
                _unpack(key, value[0])
            else:
                flat[key] = list(value)
            return

        flat[key] = value

    for top_key, top_value in record.items():
        _unpack(key=top_key, value=top_value)

    return flat


def json_to_polars(payload: Any) -> pl.LazyFrame:
    """Convert a JSON payload to a flattened Polars lazy frame.

    Args:
        payload (Any): JSON-like structure returned from the UK HPI endpoint.

    Returns:
        pl.LazyFrame: Lazy frame populated with the first set of record
        dictionaries after flattening nested mappings and single-element lists.
    """
    records = [_flatten_record(record=record) for record in _first_record_list(payload)]
    return pl.DataFrame(records).lazy()


def massage_data(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Massafe the HPI UK data

    Args:
        pl.LazyFrame: Lazy frame containing the UK HPI data.

    Returns:
        pl.LazyFrame: Massaged lazy frame.
    """
    lf = lf.select(
        pl.selectors.starts_with("ukhpi:").name.map(lambda col: col.removeprefix("ukhpi:"))
    )
    lf = lf.select(
        pl.col("refMonth_@value").str.to_date(format="%Y-%m").alias("date"),
        pl.col("refRegion_@id").str.split("/").list.get(-1).alias("region"),
        pl.selectors.starts_with("housePriceIndex")
        .cast(pl.Float64)
        .name.map(lambda col: col.replace("housePriceIndex", "hpi_").lower()),
    )
    return lf.drop("hpi_sa").rename({"hpi_": "hpi"})


def fetch_hpi_data(start_date: date, end_date: date, destination: Path) -> pl.LazyFrame:
    """Fetch and massage UK HPI data for the specified date range.

    Args:
        start_date (date): Inclusive lower bound of the series to fetch.
        end_date (date): Inclusive upper bound of the series to fetch.
        destination (Path): Filesystem location to store the downloaded Parquet.

    Returns:
        pl.LazyFrame: Massaged lazy frame containing the UK HPI data.
    """
    if destination.exists():
        print("HPI data already downloaded! Skipping download step ...")
        lf = pl.read_parquet(destination).lazy()
        return lf

    payload = request_hpi_data(start_date=date(1996, 1, 1), end_date=date(2019, 12, 1))
    lf = json_to_polars(payload=payload)
    lf = massage_data(lf=lf)
    lf.collect().write_parquet(destination)
    return lf

