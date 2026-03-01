import time
import pandas as pd
# while it worked, the 1b rows could not be loaded in 32gb of ram, the dataframe approach is not viable, even with the use of float16 which is a very low precision data type. pandas is not designed for this scale of data and its performance degrades significantly as the data size increases. dataframes are designed for flexibility for interatively working with data, not handling data of this scale
start = time.perf_counter()

df = pd.read_csv("measurements_1b.txt",sep=";",header=None)

df[1] = df[1].astype('float16')
values = df.groupby(0).agg(['min','mean','max'])

print(f"{(time.perf_counter() - start):.5}")
