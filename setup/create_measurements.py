#  Copyright 2023 The original authors
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

# Based on https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CreateMeasurements.java

import os
import sys
import random
import time
import multiprocessing
import tempfile
import shutil

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


def check_args(file_args):
    """
    Sanity checks out input and prints out usage if input is not a positive integer
    """
    try:
        if len(file_args) != 2 or int(file_args[1]) <= 0:
            raise Exception()
    except:
        print("Usage:  create_measurements.sh <positive integer number of records to create>")
        print("        You can use underscore notation for large number of records.")
        print("        For example:  1_000_000_000 for one billion")
        exit()


def build_weather_station_name_list():
    """
    Grabs the weather station names from example data provided in repo and dedups
    """
    station_names = []
    csv_path = os.path.join(SCRIPT_DIR, 'weather_stations.csv')
    with open(csv_path, 'r', encoding='utf-8') as file:
        file_contents = file.read()
    for station in file_contents.splitlines():
        if "#" in station:
            next
        else:
            station_names.append(station.split(';')[0])
    return list(set(station_names))


def convert_bytes(num):
    """
    Convert bytes to a human-readable format (e.g., KiB, MiB, GiB)
    """
    for x in ['bytes', 'KiB', 'MiB', 'GiB']:
        if num < 1024.0:
            return "%3.1f %s" % (num, x)
        num /= 1024.0


def format_elapsed_time(seconds):
    """
    Format elapsed time in a human-readable format
    """
    if seconds < 60:
        return f"{seconds:.3f} seconds"
    elif seconds < 3600:
        minutes, seconds = divmod(seconds, 60)
        return f"{int(minutes)} minutes {int(seconds)} seconds"
    else:
        hours, remainder = divmod(seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        if minutes == 0:
            return f"{int(hours)} hours {int(seconds)} seconds"
        else:
            return f"{int(hours)} hours {int(minutes)} minutes {int(seconds)} seconds"


def estimate_file_size(weather_station_names, num_rows_to_create):
    """
    Tries to estimate how large a file the test data will be
    """
    total_name_bytes = sum(len(s.encode("utf-8")) for s in weather_station_names)
    avg_name_bytes = total_name_bytes / float(len(weather_station_names))

    # avg_temp_bytes = sum(len(str(n / 10.0)) for n in range(-999, 1000)) / 1999
    avg_temp_bytes = 4.400200100050025

    # add 2 for separator and newline
    avg_line_length = avg_name_bytes + avg_temp_bytes + 2

    human_file_size = convert_bytes(num_rows_to_create * avg_line_length)

    return f"Estimated max file size is:  {human_file_size}."


_shared_station_names = None


def _worker_init(station_names):
    """Initialize each worker process with the station names list."""
    global _shared_station_names
    _shared_station_names = station_names


def _generate_chunk(args):
    """
    Worker function: generates num_rows of measurements and writes to a temp file.
    Returns the path of the temp file.
    """
    num_rows, coldest_temp, hottest_temp, tmp_path = args
    batch_size = 50_000
    written = 0
    with open(tmp_path, 'w', encoding='utf-8') as f:
        while written < num_rows:
            current_batch = min(batch_size, num_rows - written)
            stations = random.choices(_shared_station_names, k=current_batch)
            lines = '\n'.join(f"{s};{random.uniform(coldest_temp, hottest_temp):.1f}" for s in stations)
            f.write(lines + '\n')
            written += current_batch
    return tmp_path


def build_test_data(weather_station_names, num_rows_to_create):
    """
    Generates and writes to file the requested length of test data
    using multiple processes for parallel generation.
    """
    start_time = time.time()
    coldest_temp = -99.9
    hottest_temp = 99.9

    num_workers = multiprocessing.cpu_count()
    rows_per_worker = num_rows_to_create // num_workers
    remainder = num_rows_to_create % num_workers

    print(f'Building test data using {num_workers} workers...')

    # Build per-worker args with a dedicated temp file each
    tmp_files = []
    worker_args = []
    for i in range(num_workers):
        rows = rows_per_worker + (1 if i < remainder else 0)
        if rows == 0:
            continue
        fd, tmp_path = tempfile.mkstemp(suffix='.txt')
        os.close(fd)
        tmp_files.append(tmp_path)
        worker_args.append((rows, coldest_temp, hottest_temp, tmp_path))

    try:
        completed = 0
        total = len(worker_args)
        with multiprocessing.Pool(
            processes=num_workers,
            initializer=_worker_init,
            initargs=(weather_station_names,)
        ) as pool:
            for _ in pool.imap_unordered(_generate_chunk, worker_args):
                completed += 1
                bars = '=' * (completed * 50 // total)
                sys.stdout.write(f"\r[{bars:<50}] {completed * 100 // total}%")
                sys.stdout.flush()
        sys.stdout.write('\n')

        # Merge all temp files into final output
        print('Merging chunks...')
        out_path = os.path.join(SCRIPT_DIR, 'measurements.txt')
        with open(out_path, 'wb') as outfile:
            for tmp_path in tmp_files:
                with open(tmp_path, 'rb') as infile:
                    shutil.copyfileobj(infile, outfile)
    except Exception as e:
        print('Something went wrong. Printing error info and exiting...')
        print(e)
        exit()
    finally:
        for tmp_path in tmp_files:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

    end_time = time.time()
    elapsed_time = end_time - start_time
    file_size = os.path.getsize(os.path.join(SCRIPT_DIR, 'measurements.txt'))
    human_file_size = convert_bytes(file_size)

    print('Test data successfully written to measurements.txt')
    print(f'Actual file size:  {human_file_size}')
    print(f'Elapsed time: {format_elapsed_time(elapsed_time)}')


def main():
    """
    main program function
    """
    check_args(sys.argv)
    num_rows_to_create = int(sys.argv[1])
    weather_station_names = []
    weather_station_names = build_weather_station_name_list()
    print(estimate_file_size(weather_station_names, num_rows_to_create))
    build_test_data(weather_station_names, num_rows_to_create)
    print("Test data build complete.")


if __name__ == "__main__":
    main()
