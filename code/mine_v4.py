import time
import polars as pl

# while it showed promise, the 1b rows could not be loaded in 32gb of ram, the dataframe approach is not viable
# while its perfomance on the smaller sizes is way better than pandas, probably due to its rusty nature, its not scalable
start = time.perf_counter()

df = pl.scan_csv("measurements_1b.txt",separator=";",has_header=False)
values = df.group_by("column_1").agg([
    pl.col("column_2").min().alias("min"),
    pl.col("column_2").mean().alias("mean"),
    pl.col("column_2").max().alias("max")
]).collect()
print(f"{(time.perf_counter() - start):.5}")
