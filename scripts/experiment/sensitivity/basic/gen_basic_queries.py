import csv
import os
import random
import argparse
from collections import defaultdict

# Tunable parameters
NUM_EQ_PREDICATES = 5  # Number of equality queries per column
NUM_RANGE_PREDICATES = 5  # Number of range queries per numeric column
NUM_JOIN_QUERIES = 5  # Number of join queries
MAX_HOPS = 3  # Maximum hops for join queries

def parse_header(header_line):
    columns = header_line.strip().split("|")
    column_info = {}
    raw_column_names = {}
    
    for col in columns:
        name, dtype = col.split(":", 1)  # Split only on the first occurrence
        column_info[name] = dtype
        raw_column_names[name] = col  # Store the original name from CSV header
    
    return column_info, raw_column_names

def analyze_csv(file_path, column_info, raw_column_names):
    numeric_cols = {col for col, dtype in column_info.items() if dtype in {"ID(Person)", "LONG", "DATE_EPOCHMS"}}
    min_max_values = {col: [float("inf"), float("-inf")] for col in numeric_cols}
    column_values = defaultdict(set)
    
    with open(file_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter='|')
        for row in reader:
            for col, raw_col in raw_column_names.items():  # Use original column names
                value = row[raw_col]
                if col in numeric_cols:
                    value = int(value)
                    min_max_values[col][0] = min(min_max_values[col][0], value)
                    min_max_values[col][1] = max(min_max_values[col][1], value)
                else:
                    column_values[col].add(value)
    
    return min_max_values, {col: list(values) for col, values in column_values.items()}

def generate_queries(column_info, min_max_values, column_values):
    queries = []
    numeric_cols = list(min_max_values.keys())
    non_numeric_cols = [col for col in column_info.keys() if col not in numeric_cols]
    
    # Full scan
    queries.append("MATCH (n:Person) RETURN n;")
    
    # Column scan
    for col in column_info.keys():
        if col != "id":
            queries.append(f"MATCH (n:Person) RETURN n.{col};")
    
    # Equality predicate
    for col in non_numeric_cols:
        if column_values[col]:
            sample_values = random.sample(column_values[col], min(NUM_EQ_PREDICATES, len(column_values[col])))
            for val in sample_values:
                queries.append(f'MATCH (n:Person) WHERE n.{col} = "{val}" RETURN n;')
    
    # Range predicate
    for col in numeric_cols:
        min_val, max_val = min_max_values[col]
        if min_val < max_val:
            for _ in range(NUM_RANGE_PREDICATES):
                low, high = sorted(random.sample(range(min_val, max_val), 2))
                queries.append(f"MATCH (n:Person) WHERE n.{col} > {low} AND n.{col} < {high} RETURN n;")
    
    # Joins (KNOWS edge)
    for hop in range(1, MAX_HOPS + 1):
        for _ in range(NUM_JOIN_QUERIES):
            node_vars = [f"m{i}" for i in range(hop)]
            join_pattern = "-[:KNOWS]->".join([f"({var}:Person)" for var in node_vars])
    
    return queries

def save_queries(queries, output_folder):
    os.makedirs(output_folder, exist_ok=True)
    
    for i, query in enumerate(queries, start=1):
        with open(os.path.join(output_folder, f"q{i}.cql"), "w", encoding="utf-8") as f:
            f.write(query + "\n")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("csv_file", help="Input CSV file")
    parser.add_argument("output_folder", help="Output folder for queries")
    args = parser.parse_args()
    
    with open(args.csv_file, "r", encoding="utf-8") as f:
        header_line = f.readline().strip()
    
    column_info, raw_column_names = parse_header(header_line)
    min_max_values, column_values = analyze_csv(args.csv_file, column_info, raw_column_names)
    queries = generate_queries(column_info, min_max_values, column_values)
    save_queries(queries, args.output_folder)
    print(f"Generated {len(queries)} queries in {args.output_folder}")

if __name__ == "__main__":
    main()
