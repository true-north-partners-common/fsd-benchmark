# FSD Data Pipeline

This project assembles an end-to-end extract, transform, and load (ETL) pipeline for analysing
repeat-sale property transactions in the UK. It downloads public datasets, normalises them and
computes the forced sale discount (FSD) metrics that highlight price differences between consecutive
sales of the same property.

The present project attempts to follow the methodology in
(https://cer.business-school.ed.ac.uk/wp-content/uploads/sites/55/2019/07/Using-HMLR-Data-to-Estimate-Forced-Sale-Discount.pdf)
to calculate an FSD benchmark for the UK.

## Project Structure

- `src/main.py` — entrypoint that orchestrates the ETL run by fetching raw datasets and
  invoking the processing pipeline.
- `src/pipeline.py` — high-level merge and transformation logic combining price paid, house price
  index, and postcode reference data.
- `src/pp.py` — helpers for downloading and preparing UK Land Registry price paid transaction
  records.
- `src/hpi.py` — utilities for fetching and flattening the UK House Price Index API payload.
- `src/postcode_lookup.py` — postcode area lookup loader used to expand partial postcodes into
   regions.
- `src/calculate_fsd.py` — standalone module that exposes the same merge-and-calculate routine for
  reuse by other tools.
- `data/` — default directory where downloaded CSV and Parquet assets are cached for subsequent
  runs.

## Data Sources

- [**UK Land Registry Price Paid Data**](http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.csv):
  full historical transaction log used as the backbone of the analysis.
- [**UK House Price Index (HPI)**](https://landregistry.data.gov.uk/app/ukhpi/download/new.json):
  monthly indices per nation and region that contextualise transaction prices and provide inflation
  adjustments.
- [**UK Postcode Area Lookup**](https://www.roblocher.com/technotes/uk-postareas.html):
  HTML table mapping postcode prefixes to geographic regions for consistent joins across datasets.

## Exclusions

The following exclusions are applied:

- Any address not sold more than once between 1995 and 2019
- Property type "O" (other)
- Sale price < £10,000
- Transaction type B < October 2013
- Time between transactions < 12 months

## Methodology

The calculation follows the approach detailed in the HMLR forced sale discount research. For each
property that sells multiple times, the pipeline selects a pair where the later transaction is
categorised as type *B* and the earlier sale is type *A*.

> [!NOTE]
> Refer to https://www.gov.uk/guidance/about-the-price-paid-data for a data dictionary describing
> type B and A

> [!IMPORTANT]
> Type B is described as "Additional Price Paid entry including transfers under a power of
> sale/repossessions, buy-to-lets (where they can be identified by a Mortgage), transfers to
> non-private individuals and sales where the property type is classed as ‘Other’". Therefore,
> type B transactions are not necessarily a result of a repossession.

The earlier price is indexed forward using the appropriate regional house price index—mapped from
the postcode—and the property type, producing a counterfactual price that would have been expected
without a forced sale. Comparing this indexed value with the observed resale price yields the basic
fast sale discount. A conservative discount is also derived by reducing the indexed benchmark by
five percent before performing the comparison, so both aggressive and cautious estimates are
available for analysis. Finally, all negative FSD are excluded.

> [!IMPORTANT]
> Negative FSDs are excluded from the average FSD calculation under the assumption that repossessed
> properties are sold at price lower that the property would have otherwise been sold under
> different circumstances. Type B transactions resulting in a negative FSD are assumed to not be
> transactions relating to repossessions.