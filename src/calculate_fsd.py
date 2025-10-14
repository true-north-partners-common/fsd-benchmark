import polars as pl


def merge_and_process_data(
    pp_lf: pl.LazyFrame,
    hpi_lf: pl.LazyFrame,
    postcode_lookup_lf: pl.LazyFrame,
) -> tuple[pl.LazyFrame, pl.DataFrame]:
    """Merge source datasets and compute FSD metrics.

    Args:
        pp_lf (pl.LazyFrame): Price paid transactions with lagged price fields.
        hpi_lf (pl.LazyFrame): Regional house price index observations.
        postcode_lookup_lf (pl.LazyFrame): Postcode to region lookup reference.

    Returns:
        tuple[pl.LazyFrame, pl.DataFrame]:
            The augmented transaction lazy frame and the aggregated FSD summary.
    """
    pp_with_regions = pp_lf.join(
        postcode_lookup_lf,
        left_on=pl.col("postcode").str.slice(offset=0, length=2),
        right_on="postcode_area",
        how="left",
        validate="m:1",
    )
    pp_with_regions = pp_with_regions.with_columns(
        pl.col("country").fill_null("unknown").alias("region")
    )
    pp_with_regions = pp_with_regions.drop("country")

    merged = hpi_lf.join(
        pp_with_regions,
        right_on=[pl.col("region"), pl.col("transfer_date").dt.month_end()],
        left_on=[pl.col("region"), pl.col("date").dt.month_end()],
        how="left",
        validate="1:m",
    )
    merged = merged.drop("region_right", "date")
    merged = merged.with_columns(
        pl.when(pl.col("new_build"))
        .then(pl.col("hpi_newbuild"))
        .when(pl.col("property_type") == "detached")
        .then(pl.col("hpi_detached"))
        .when(pl.col("property_type") == "semi-detached")
        .then(pl.col("hpi_semidetached"))
        .when(pl.col("property_type") == "terraced")
        .then(pl.col("hpi_terraced"))
        .when(pl.col("property_type") == "flat/maisonette")
        .then(pl.col("hpi_flatmaisonette"))
        .otherwise("hpi")
        .alias("hpi")
    )

    drop_cols = [col for col in merged.collect_schema().keys() if col.startswith("hpi_")]
    merged = merged.drop(drop_cols)

    merged = merged.with_columns(
        pl.col("hpi").shift(1).over("full_address").name.suffix("_lag")
    )
    merged = merged.with_columns(pl.col("hpi").truediv("hpi_lag").alias("hpi_ratio"))
    merged = merged.filter(pl.col("hpi_ratio").is_not_null())
    merged = merged.with_columns(pl.col("price_lag").mul("hpi_ratio").name.keep())
    merged = merged.filter(
        (pl.col("ppd_category").eq("b") & pl.col("ppd_category_lag").eq("a"))
    )

    merged = merged.with_columns(
        pl.col("price_lag")
        .sub(pl.col("price"))
        .truediv(pl.col("price_lag"))
        .alias("fsd"),
        pl.col("price_lag")
        .sub(pl.col("price") - pl.col("price_lag").mul(pl.lit(0.05)))
        .truediv(pl.col("price_lag"))
        .alias("fsd_conservative"),
    )

    fsd_summary = (
        merged.filter(pl.col("fsd") > 0)
        .group_by("region")
        .agg(pl.col("fsd").mean(), pl.col("fsd_conservative").mean())
        .collect()
    )

    return merged, fsd_summary
