import os
import re
import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import gmean
import argparse

def extract_compile_time(file_path):
    with open(file_path, 'r') as file:
        content = file.read()
        match = re.search(r"Average Compile Time:\s*(\d+\.\d+)\s*ms", content)
        if match:
            return float(match.group(1))
        else:
            return None

def calculate_slowdowns(folder_path):
    schemas = [1, 2, 4, 8]
    queries = [1, 2, 3, 4, 5, 6, 8]  # Specified queries
    slowdown_data = {'with': {schema: [] for schema in schemas},
                     'without': {schema: [] for schema in schemas}}
    
    for query in queries:
        baseline_time_with = extract_compile_time(os.path.join(folder_path, f"with-coalescing-Q{query}-schema-1.txt"))
        baseline_time_without = extract_compile_time(os.path.join(folder_path, f"without-coalescing-Q{query}-schema-1.txt"))

        if baseline_time_with is None or baseline_time_without is None:
            continue

        for schema in schemas:
            time_with = extract_compile_time(os.path.join(folder_path, f"with-coalescing-Q{query}-schema-{schema}.txt"))
            time_without = extract_compile_time(os.path.join(folder_path, f"without-coalescing-Q{query}-schema-{schema}.txt"))

            if time_with is not None:
                slowdown_with = time_with / baseline_time_with
                slowdown_data['with'][schema].append(slowdown_with)
            
            if time_without is not None:
                slowdown_without = time_without / baseline_time_without
                slowdown_data['without'][schema].append(slowdown_without)
    
    return slowdown_data

def plot_slowdowns(slowdown_data, line_styles):
    schemas = [1, 2, 4, 8]
    avg_slowdowns_with = []
    avg_slowdowns_without = []

    for schema in schemas:
        avg_slowdowns_with.append(gmean(slowdown_data['with'][schema]))
        avg_slowdowns_without.append(gmean(slowdown_data['without'][schema]))

    plt.figure(figsize=(7, 5))
    
    plt.plot(schemas, avg_slowdowns_with, marker='x', linestyle=line_styles['with']['linestyle'], color=line_styles['with']['color'], label='W/ Coalescing', linewidth=2, markersize=10)
    plt.plot(schemas, avg_slowdowns_without, marker='o', linestyle=line_styles['without']['linestyle'], color=line_styles['without']['color'], label='W/O Coalescing', linewidth=2, markersize=10)
    
    plt.xlabel('Number of Schemas', fontsize=22, labelpad=11)
    plt.ylabel('Normalized\nCompilation Time', fontsize=22)
    plt.xticks(schemas, fontsize=19)
    plt.ylim(0, 30)
    plt.yticks(np.arange(0, 30.01, step=5), fontsize=19)
    plt.legend(fontsize=19)

    # Calculate the value difference for schema-8
    schema_index = schemas.index(8)
    value_difference = 14.4

    # Get the y-coordinates for schema-8 in both series
    y_with = avg_slowdowns_with[schema_index]
    y_without = avg_slowdowns_without[schema_index]

    # Define a small gap
    gap = -0.2

    # Add a bidirectional arrow between the two points for schema-8 with a small gap
    plt.annotate('', xy=(8, y_with - gap), xytext=(8, y_without + gap),
                 arrowprops=dict(arrowstyle='<->', color='blue', lw=2))
    # Optionally, add a text annotation for the value difference
    plt.text(7.8, (y_with + y_without) / 2, '14.4x', color='blue', ha='right', va='center', fontsize=19)

    plt.tight_layout()
    plt.savefig('coalesce-graph.pdf', bbox_inches='tight')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate a line graph from compilation time data.')
    parser.add_argument('folder', type=str, help='Path to the folder containing the data files.')
    args = parser.parse_args()

    folder_path = args.folder

    # Calculate slowdowns
    slowdown_data = calculate_slowdowns(folder_path)

    # Define line styles
    line_styles = {
        'with': {'linestyle': '-', 'color': 'black'},
        'without': {'linestyle': '--', 'color': 'gray'}
    }

    # Plot the results
    plot_slowdowns(slowdown_data, line_styles)
