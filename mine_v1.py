from pprint import *
import time
start = time.perf_counter()
# the naive baseline for this problem, did not even wait to finish after 1m rows, since it was taking so long but a good starting point nonetheless

with open("measurements_1k.txt",'r', encoding='utf-8') as file:
    measurements = {}
    for line in file:
        city,value = line.strip().split(";")
        if city not in measurements.keys():
            measurements[city] = [float(value),float(value),float(value),1]
        else:
            value = float(value)
            curr_min,curr_total,curr_max,count = measurements[city]
            measurements[city] = [min(curr_min,value),curr_total + value, max(curr_max,value),count+1]
for k in measurements:
    measurements[k][1] = measurements[k][1]/measurements[k][3]
    measurements[k].pop()

print(f"{(time.perf_counter() - start):.5}")
