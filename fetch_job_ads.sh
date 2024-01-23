#!/bin/bash

# Load line oriented json job ads from Arbetsförmedlingen open data store,
# parse them and store extracted data in parquet files.
# It requires wget, unzip and python3.
# On average 5Gb of JSON data is downloaded per year.

# Base URL for the files
BASE_URL="https://data.jobtechdev.se/annonser/historiska/"


process_file() {
    echo "working on $1"
    year=$1
    file_url="${BASE_URL}${year}.jsonl.zip"
    zip_file="${year}.jsonl.zip"
    jsonl_file="${year}.jsonl"

    # Fetch and unzip the zipfile
    wget -q "$file_url" -O "$zip_file"
    unzip -q "$zip_file"

    # Run the Python script on the JSONL file
    python make_parquet_from_json.py "$jsonl_file" "${year}_remote.parquet"

    # Clean up
    rm "$zip_file" "$jsonl_file"
}

# Loop through the years 2016 to 2023 and process files
for year in {2016..2023}; do
    process_file "$year" &
done

wait

echo "All files processed."