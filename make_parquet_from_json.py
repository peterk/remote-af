"""
This script reads a job ad JSON lines dump file in chunks, processes the
data, and writes it to a Parquet file.  It combines the 'headline' and
'description' fields into a 'combined_text' field, detects remote work
opportunities, extracts the 'municipality_code' from the 'workplace_address',
and converts the 'publication_date' to a datetime object.  The resulting data is
then written to a Parquet file with Snappy compression.

Job ad dumps are available at https://data.jobtechdev.se/annonser/historiska/index.html
"""
import sys

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

CHUNKSIZE = 10000
parquet_writer = None


if __name__ == '__main__':
    file_path = sys.argv[1] # Path to the JSON file
    output_file_path = sys.argv[2] # Path to the output Parquet file

    row = 0

    for chunk in pd.read_json(file_path, lines=True, chunksize=CHUNKSIZE):
        chunk['combined_text'] = chunk.apply(
            lambda x: (str(x['headline']) + ' ' + str(
                x['description'].get('text') if isinstance(x['description'], dict) else '')).lower(), axis=1)
        
        chunk['combined_text'] = chunk['combined_text'].str.replace('\s+', ' ', regex=True).str.replace('\*{2,}', '*', regex=True)
        chunk['source_file'] = file_path

        chunk['row_number'] = range(row, row + len(chunk))
        row += len(chunk)

        # Fancy remote work detection
        chunk['is_remote'] = chunk['combined_text'].apply(
            lambda x: "jobba på distans" in x or "arbeta hemifrån" in x or "möjlighet att distansarbeta" in x or "jobba hemifrån" in x)
        
        chunk['municipality_code'] = chunk['workplace_address'].apply(
            lambda x: x.get('municipality_code') if isinstance(x, dict) else None)
        
        chunk['publication_date'] = pd.to_datetime(chunk['publication_date'])
        
        # Drop unnecessary columns
        chunk = chunk[['source_file', 'row_number', 'municipality_code', 'publication_date', 'is_remote']]
        
        table = pa.Table.from_pandas(chunk)

        if parquet_writer is None:
            parquet_schema = table.schema
            parquet_writer = pq.ParquetWriter(output_file_path, parquet_schema, compression='snappy')

        parquet_writer.write_table(table)

    if parquet_writer is not None:
        parquet_writer.close()