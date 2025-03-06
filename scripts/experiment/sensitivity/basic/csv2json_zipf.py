import csv
import json
import math
import random
import argparse
import itertools
import os
import matplotlib.pyplot as plt
from collections import Counter

def parse_csv(file_path, delimiter='|'):
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        return list(reader), reader.fieldnames

def clean_column_name(column):
    return column.split(':')[0]

def extract_label_from_id(columns):
    for col in columns:
        if col.startswith("id:ID(") and col.endswith(")"):
            return col[col.index("(") + 1 : col.index(")")]
    return "NODE"

def extract_column_types(columns):
    """Extract column types from column names."""
    column_types = {}
    for col in columns:
        parts = col.split(':')
        col_name = parts[0]
        col_type = parts[1] if len(parts) > 1 else "STRING"
        column_types[col_name] = col_type
    return column_types

def generate_schema_mapping_and_indices_rand_num_column(alpha, num_objects, columns):
    seed = 42
    random.seed(seed)
    
    first_column = 0  # The first column is required
    other_columns = list(range(1, len(columns)))  # Exclude the first column
    all_combinations = []
    
    # Generate all possible non-empty subsets of other columns
    for r in range(0, len(other_columns) + 1):
        for comb in itertools.combinations(other_columns, r):
            all_combinations.append((first_column,) + comb)  # Ensure first column is included

    num_schemas = len(all_combinations)
    
    schema_mapping = {}
    for schema_index, combination in enumerate(all_combinations):
        schema_mapping[schema_index] = list(combination)

    if num_schemas == 1:
        schema_indices = [0] * num_objects
    else:
        schema_indices = sample_zipf_values(alpha, num_objects, num_schemas - 1)
    
    return schema_mapping, schema_indices

def NextRand(nSeed):
    nA, nM, nQ, nR = 16807, 2147483647, 127773, 2836
    nU = nSeed // nQ
    nV = nSeed - nQ * nU
    nSeed = nA * nV - nU * nR
    if nSeed < 0:
        nSeed += nM
    return nSeed

def UnifInt(nLow, nHigh, seed):
    if nLow == nHigh:
        return nLow, seed
    dRange = nHigh - nLow + 1
    seed = NextRand(seed)
    nTemp = int((seed / 2147483647.0) * dRange)
    return nLow + nTemp, seed

def GetMultiplier(n, zipf):
    if zipf == 0.0:
        return n
    elif zipf == 1.0:
        return math.log(n) + 0.577
    elif zipf == 2.0:
        return (math.pi ** 2) / 6.0
    elif zipf == 3.0:
        return 1.202
    elif zipf == 4.0:
        return (math.pi ** 4) / 90.0
    else:
        multiplier = sum(1.0 / (i ** zipf) for i in range(1, n + 1) if 1.0 / (i ** zipf) > 1e-4)
        return multiplier

def SkewInt(nLow, nHigh, skewVal, n, seed, state):
    if state["NumDistinctValuesGenerated"] == 0:
        state["Multiplier"] = GetMultiplier(n, skewVal)
    
    if state["CurrentValueCounter"] == state["CurrentValueTarget"]:
        seed = NextRand(seed)
        dRange = nHigh - nLow + 1
        nTemp = int((seed / 2147483647.0) * dRange) + nLow
        state["CurrentValue"] = nTemp
    else:
        nTemp = state["CurrentValue"]
        state["CurrentValueCounter"] += 1
        return nTemp, seed, state
    
    state["NumDistinctValuesGenerated"] += 1
    Czn = n / state["Multiplier"]
    state["CurrentValueTarget"] = max(int(Czn / (state["NumDistinctValuesGenerated"] ** skewVal)), 1)
    state["CurrentValueCounter"] = 1
    return nTemp, seed, state

def sample_zipf_values(alpha, num_samples, max_value):
    seed = 1
    state = {"CurrentValue": 0, "NumDistinctValuesGenerated": 0, "CurrentValueTarget": 0, "CurrentValueCounter": 0, "Multiplier": 0.0}
    samples = []
    for _ in range(num_samples):
        if alpha == 0:
            value, seed = UnifInt(0, max_value, seed)
        else:
            value, seed, state = SkewInt(0, max_value, alpha, num_samples, seed, state)
        samples.append(value)
    return samples

def plot_schema_distribution(schema_indices):
    schema_counts = Counter(schema_indices)
    sorted_counts = sorted(schema_counts.items(), key=lambda x: x[1])  # Sort by frequency
    schemas, counts = zip(*sorted_counts)

    plt.figure(figsize=(10, 5))
    plt.bar(range(len(schemas)), counts)
    plt.xlabel("Schema Index (sorted by frequency)")
    plt.ylabel("Number of Tuples")
    plt.title("Schema-to-Tuple Distribution")
    plt.savefig("distribution.png")
    plt.close()
    print("Distribution graph saved as distribution.png")

def generate_zipf_filename(file_path: str, zipf_value: int) -> str:
    base_name = os.path.splitext(os.path.basename(file_path))[0]
    new_filename = f"{base_name}-zipf-{zipf_value}.json"
    return new_filename

def parse_value(value, column_type):
    """Convert value based on its type."""
    if "ID" in column_type or column_type == "LONG" or column_type == "DATE_EPOCHMS":
        return int(value) if value.isdigit() else None
    return value  # Default to string

def process_csv_to_json(csv_file, zipf_value, output_file, plot_graph):
    rows, columns = parse_csv(csv_file)
    cleaned_columns = [clean_column_name(col) for col in columns]
    label = extract_label_from_id(columns)
    column_types = extract_column_types(columns)

    schema_mapping, schema_indices = generate_schema_mapping_and_indices_rand_num_column(zipf_value, len(rows), columns)
    
    json_output = []
    for idx, row in enumerate(rows):
        schema_idx = schema_indices[idx]
        selected_columns = schema_mapping[schema_idx]
        
        filtered_row = {cleaned_columns[i]: 
            parse_value(row[columns[i]], column_types[cleaned_columns[i]]) for i in selected_columns if columns[i] in row}
        json_output.append({"labels": [label], "properties": filtered_row})
    
    with open(output_file, 'w', encoding='utf-8') as f:
        for entry in json_output:
            f.write(json.dumps(entry) + "\n")
    
    print(f"Output written to {output_file}")
    
    if plot_graph:
        plot_schema_distribution(schema_indices)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert CSV to JSON with Zipf distribution-based schema modification.")
    parser.add_argument("csv_file", type=str, help="Input CSV file path")
    parser.add_argument("zipf_value", type=int, choices=range(5), help="Zipf distribution value (0 for uniform, 1-4 for skewed distributions)")
    parser.add_argument("--plot", action="store_true", help="Generate schema distribution graph")
    
    args = parser.parse_args()
    process_csv_to_json(args.csv_file, args.zipf_value, generate_zipf_filename(args.csv_file, args.zipf_value), args.plot)
