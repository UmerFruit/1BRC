import time
from multiprocessing import Pool
import os
# DICTIONARY LOOKUPS while initially seemed expensive, did not effect the results that much

def worker(args):
    start, end = args
    with open(file_name,'rb') as file:
        file.seek(start)
        if start != 0:
            file.readline() 
        real_start = file.tell()
        partial_m = {}
        while file.tell() < end:
            line = file.readline().decode('utf-8')
            city,value = line.strip().split(";")
            entry = partial_m.get(city)
            if entry is None:
                partial_m[city] = [float(value),float(value),float(value),1]
            else:
                value = float(value)
                curr_min,curr_total,curr_max,count = entry
                partial_m[city] = [min(curr_min,value),curr_total + value, max(curr_max,value),count+1]
             
        return partial_m

def merger(results):
    final = {}

    for p_dict in results:
        for city in p_dict:
            entry = final.get(city)
            if entry is None:
                final[city] = p_dict[city]
            else:
                c_min,total,c_max,count = entry
                final[city] = [min(c_min,p_dict[city][0]), 
                               total + p_dict[city][1], 
                               max(c_max,p_dict[city][2]),
                               count+p_dict[city][3]]
        
    for k in final:
            final[k][1] = final[k][1]/final[k][3]    
            final[k].pop()
    return final
    
        

file_name = "measurements_1b.txt"

if __name__ == "__main__":
    start = time.perf_counter()
    size = os.path.getsize(file_name)
    n = 20
    
    chunk = size // n
    starts = [i * chunk for i in range(n)]
    ends = [i * chunk for i in range(1, n)] + [size]
    chunks = list(zip(starts,ends))
    with Pool(n) as p:  
        results = p.map(worker, chunks)
        result = merger(results)
    print(f"{(time.perf_counter() - start):.5}")
