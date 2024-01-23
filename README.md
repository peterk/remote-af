# remote-af
Quick analysis sketch of Swedish remote job ads using [DuckDB](https://duckdb.org/), [PyArrow](https://arrow.apache.org/docs/python/index.html) and Parquet. 

Please note that each year will download approximately 5GB json data.

## 1. Install Python dependencies

```
pip install -r requirements.txt
```

## 2. fetch_job_ads.sh
JSON line oriented data files are fetched from the [Swedish Employment Agency's open data store](https://data.jobtechdev.se/annonser/historiska/index.html).

For each downloaded json dump file, the make_parquet_from_json.py script is called, creating one parquet file for each year.

## 3. make_graph.py
Use DuckDB to query the parquet files and plot the cart.