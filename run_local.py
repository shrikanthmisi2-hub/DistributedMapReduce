"""
Local execution driver for MapReduce Word Count.
Simulates the Map, Shuffle/Sort, and Reduce phases locally.
"""

import os
import sys
from collections import defaultdict
from workers.mapper import map_function
from workers.reducer import reduce_function

def shuffle_and_group(mapped_pairs):
    """
    Groups the mapped (key, value) pairs by key.
    Returns a dictionary mapping word -> list of counts.
    """
    grouped_data = defaultdict(list)
    for key, value in mapped_pairs:
        grouped_data[key].append(value)
    return grouped_data

def main():
    input_file = os.path.join("datasets", "sample.txt")
    output_file = os.path.join("output", "result.txt")

    # Ensure dataset exists
    if not os.path.exists(input_file):
        print(f"Error: Input file {input_file} not found.", file=sys.stderr)
        sys.exit(1)

    print("--- Starting Local MapReduce ---")

    # 1. Read input
    try:
        with open(input_file, "r", encoding="utf-8") as f:
            lines = f.readlines()
    except Exception as e:
        print(f"Error reading {input_file}: {e}", file=sys.stderr)
        sys.exit(1)

    # Handle empty files
    if not lines:
        print("Warning: Input file is empty. Writing empty output.")
        with open(output_file, "w", encoding="utf-8") as f:
            pass
        sys.exit(0)

    # 2. Map Phase
    print("Executing Map phase...")
    mapped_pairs = []
    for line in lines:
        mapped_pairs.extend(map_function(line))

    # 3. Shuffle & Group Phase
    print("Executing Shuffle & Group phase...")
    grouped_data = shuffle_and_group(mapped_pairs)

    # 4. Reduce Phase
    print("Executing Reduce phase...")
    reduced_results = []
    for key, values in grouped_data.items():
        reduced_results.append(reduce_function(key, values))

    # 5. Sort final output alphabetically by word
    reduced_results.sort(key=lambda x: x[0])

    # 6. Write Output
    try:
        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        with open(output_file, "w", encoding="utf-8") as f:
            for key, count in reduced_results:
                f.write(f"{key}: {count}\n")
        print(f"Success: Results written to {output_file}")
    except Exception as e:
        print(f"Error writing to {output_file}: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
