# 1 Billion Row Challenge (1BRC) — Python Exploration

My personal exploration of the [1 Billion Row Challenge](https://github.com/gunnarmorling/1brc): read a ~13 GB text file containing 1,000,000,000 weather station measurements and compute the **min / mean / max** temperature for each station as fast as possible, using only Python.

---

## Problem Statement

The input file has the format:

```
Hamburg;12.0
Bulawayo;8.9
Palembang;38.8
...
```

The goal is to produce a result of the form:

```
{city: [min, mean, max], ...}
```

for all ~400 unique weather stations across the full 1-billion-row dataset.

---

## Data Generation

While the original 1BRC file was provided i made my own edits to it, making it multithreaded so that i could generate the files faster for testing, also to generate files of smaller sized for testing the performance of the different approaches on different sizes of data.

Use `create_measurements.py` to generate test files of any size:

```bash
python create_measurements.py <num_rows>
```
If you want to copy my personal approach, you can run these commands:
```bash
python create_measurements.py 1_000
python create_measurements.py 100_000
python create_measurements.py 1_000_000
python create_measurements.py 100_000_000
python create_measurements.py 1_000_000_000
```
Note that these generate files with the same name and will overwrite each other. I renamed them after running each command so be mindful of that when just copy pasting commands from here. You can also modify the `create_measurements.py` script to generate files with different names if you want to avoid this issue.

Generated files used for benchmarking:

| File | Rows |
|------|------|
| `measurements_1k.txt` | 1,000 |
| `measurements_100k.txt` | 100,000 |
| `measurements_1m.txt` | 1,000,000 |
| `measurements_100m.txt` | 100,000,000 |
| `measurements_1b.txt` | 1,000,000,000 |

---

## Versions

### `mine_v1.py` — Naive Baseline
Pure Python, single-threaded, line-by-line dict accumulation.  
Too slow to finish the 1B file — estimated ~1000s. Useful only as a correctness reference.

### `mine_v2.py` — Multiprocessing + Chunking
Splits the file into 20 equal byte-range chunks and processes each in a separate process using `multiprocessing.Pool`. Results are merged in a final pass.  
First meaningful attempt at parallelism. ~404s on 1B rows.

### `mine_v2.1.py` — Dict Lookup Optimisation
Replaces `city not in d.keys()` with `d.get(city)` to avoid repeated key iteration.  
Contrary to expectation, made no significant improvement (~445s) — the bottleneck lay elsewhere.

### `mine_v2.2.py` — Remove `file.tell()` Bottleneck
`file.tell()` was called every iteration to track position — a surprisingly expensive syscall. Replaced with a manual byte counter (`current += len(line)`).  
**Best multiprocessing result: ~300s on 1B rows.**

### `mine_v2.3.py` — Validate Dict Lookup Finding
Re-applies the byte-counter fix from v2.2 but reverts to `not in .keys()` lookup, to confirm whether the dict optimisation was genuinely helpful.  
Result: ~336s — confirms the byte-counter was the real win.

### `mine_v3.py` — Pandas DataFrame
Loads the entire file into a Pandas DataFrame and uses `groupby().agg()`.  
Convenient but not scalable: the 1B-row file could not be fully loaded into 32 GB RAM, and Pandas is optimised for interactive analysis, not bulk ingestion at this scale. ~515s.  
Also explored casting column 1 to `float16` to reduce memory pressure.

### `mine_v3.1.py` — Pandas with Chunked Reading
Reads the file in 100M-row chunks, aggregates each chunk, then concatenates and re-aggregates.  
Reduces peak RAM but the final `pd.concat` + re-groupby becomes the new bottleneck. ~360s on 1B rows.

### `mine_v4.py` — Polars (Lazy Evaluation)
Uses Polars' `scan_csv` (lazy API backed by Rust) so only the required computation is materialised.  
Dramatically faster than Pandas on small sizes. Still hits memory limits on 1B rows in 32 GB RAM. ~30s on 1B rows.

### `mine_v5.py` — DuckDB ⭐ Winner
Queries the CSV file directly with SQL — no in-process loading, fully out-of-core:

```python
duckdb.sql("SELECT column0, MIN(column1), AVG(column1), MAX(column1) "
           "FROM read_csv('measurements_1b.txt', sep=';', header=false) "
           "GROUP BY column0").fetchall()
```

**Winner by a huge margin: ~23s on 1B rows, with very low memory usage.**

---

## Benchmark Results

All times are in **seconds**. `~` denotes an estimate (run was stopped early or extrapolated).

| Version | 1k | 100k | 1m | 100m | 1b |
|---------|----|------|----|------|----|
| `mine_v1` | 0.001 | 0.106 | 0.933 | ~100 | ~1000 |
| `mine_v2` | 0.131 | 0.269 | 1.016 | 21.99 | 404.57 |
| `mine_v2.1` | 0.142 | 0.299 | 1.010 | 22.94 | 445.12 |
| `mine_v2.2` | 0.126 | 0.269 | 0.947 | 31.80 | **300.11** |
| `mine_v2.3` | 0.149 | 0.282 | 0.896 | 14.91 | 336.08 |
| `mine_v3` | 0.005 | 0.054 | 0.366 | 37.96 | 515.35 |
| `mine_v3.1` | 0.006 | 0.067 | 0.351 | 36.35 | ~360 |
| `mine_v4` | 0.009 | 0.011 | 0.031 | 3.16 | ~30 |
| `mine_v5` | 0.013 | 0.050 | 0.144 | 2.92 | **23.46** |

---

## Key Takeaways

- **`file.tell()` is expensive** at scale — replacing it with a manual byte counter cut 1B processing time from ~405s to ~300s.
- **Dict lookup micro-optimisations** (`get()` vs `in .keys()`) had negligible impact.
- **Pandas is not designed for this scale** — memory pressure and slow I/O make it unsuitable for 1B rows.
- **Polars** (lazy Rust-backed evaluation) is a massive improvement over Pandas but still constrained by available RAM.
- **DuckDB** sidesteps the memory problem entirely by treating the file as a queryable table, making it the clear winner.

---

## Requirements

```
pandas
polars
duckdb
```

Install with:

```bash
pip install pandas polars duckdb
```
