# Generate a Parquet file using DuckDB.
duckdb :memory: << EOF
COPY (
  SELECT *
  FROM read_parquet('data/master.parquet')
  LIMIT 50
) TO STDOUT (FORMAT 'parquet', COMPRESSION 'gzip');
EOF