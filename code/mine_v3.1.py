import time
import pandas as pd
# everything critique previous version is still applicable, an attempt to speed it up by chunking the file but then the bottleneck became the concatination of the dataframes
start = time.perf_counter()

chunks = []
for chunk in pd.read_csv("measurements_100m.txt", sep=";", header=None, chunksize=100_000_000):
    chunks.append(chunk.groupby(0)[1].agg(['min', 'mean', 'max']))

result = pd.concat(chunks).groupby(level=0).agg({'min': 'min', 'mean': 'mean', 'max': 'max'})
print(f"{(time.perf_counter() - start):.5}")