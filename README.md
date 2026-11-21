# US Accidents Analytics

An Azure data-engineering workflow that turns US traffic-accident records into partitioned Parquet datasets for schema inspection, data-quality analysis, and exploratory analytics.

**Azure Data Factory · Azure Data Lake Storage Gen2 · Databricks · PySpark · Parquet**

## Architecture

```text
US Accidents CSV → raw zone → Data Factory transformations
                → processed Parquet → Databricks cleaning
                → curated features → exploratory analysis
```

The reference PySpark job derives year, month, hour of day, weekday rush-hour flags, and weather-condition categories. Outputs are partitioned by accident year.

## Repository guide

| File | Purpose |
| --- | --- |
| [`src/etl_process.py`](src/etl_process.py) | Executable reference ETL with configurable input/output paths |
| `src/01_Load_and_Clean _Accidents.ipynb` | Cleaning and feature preparation |
| `src/02_metadata_and_schema.ipynb.ipynb` | Schema inspection and missing-value analysis |
| `src/03_EDA.ipynb.ipynb` | Exploratory analysis of temporal and weather features |

## Run the reference ETL

Use Python 3.11, Java 17, and the US Accidents March 2023 CSV obtained under its original dataset terms. Large datasets are not included.

```sh
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python src/etl_process.py --input /path/to/US_Accidents_March23.csv --output ./processed/accidents
```

The output directory is overwritten on each run. For Azure execution, use your own `abfss://` paths and configure access in your Databricks workspace. Update notebook storage settings before running the notebooks in order. Data Factory resources are described by the workflow; an infrastructure deployment template is not included.

## Scope and interpretation

The repository covers ETL and exploratory analysis. It does not contain a trained accident-severity model or a deployed prediction API. Weather categories are a coarse feature mapping, and observational patterns do not establish causes of accidents.
