import duckdb
import time
# a highly performant analytical database and a query engine used to directly query files without loading them in memory. the winner by a huge margin with very low memory usage.
start = time.perf_counter()

# query a file directly like a table
# result = duckdb.sql("SELECT * FROM read_csv('measurements_1k.txt', sep=';', header=false)")
result = duckdb.sql("SELECT column0, MIN(column1), AVG(column1), MAX(column1) FROM read_csv('measurements_100m.txt', sep=';', header=false) GROUP BY column0").fetchall()

print(f"{time.perf_counter() - start:.5f}")