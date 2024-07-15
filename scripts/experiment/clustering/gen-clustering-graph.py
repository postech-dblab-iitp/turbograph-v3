import os
import re
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from collections import defaultdict
from scipy.stats import gmean
import sys

def extract_times(file_path):
    with open(file_path, 'r') as file:
        content = file.read()
        try:
            time1 = float(re.search(r'Average Query Execution Time: (\d+(\.\d+)?) ms', content).group(1))
            time2 = float(re.search(r'Average Compile Time: (\d+(\.\d+)?) ms', content).group(1))
        except AttributeError:
            # If either time1 or time2 is not found, skip this file
            return None
    return time1 + time2

def read_logs(folder):
    data = []
    for filename in os.listdir(folder):
        if filename.startswith('goodbye_zipf') and filename.endswith('.txt'):
            parts = filename.split('_')
            distribution = parts[2]
            query_number = parts[3]
            algorithm = parts[4]
            measure = parts[5]
            # Correctly handle the case where layering can contain underscores
            layering = '_'.join(parts[6:]).replace('.txt', '')
            total_time = extract_times(os.path.join(folder, filename))
            if total_time is not None:
                if distribution != "0":
                    data.append((distribution, query_number, algorithm, measure, layering, total_time))
    return data

def calculate_geomean_speedups(data, mode):
    df = pd.DataFrame(data, columns=['Distribution', 'Query', 'Algorithm', 'Measure', 'Layering', 'TotalTime'])

    if mode == 'measure':
        baseline_measure = 'OURS'
        measures = ['OURS', 'OVERLAP', 'JACCARD', 'WEIGHTEDJACCARD', 'COSINE', 'DICE']
        baseline_times = df[df['Measure'] == baseline_measure].groupby(['Distribution', 'Query'])['TotalTime'].mean()
        speedups = defaultdict(lambda: defaultdict(list))
        for measure in measures:
            measure_times = df[df['Measure'] == measure].groupby(['Distribution', 'Query'])['TotalTime'].mean()
            for dist_query in baseline_times.index:
                if dist_query in measure_times.index:
                    speedup = measure_times[dist_query] / baseline_times[dist_query]
                    speedups[dist_query[0]][measure].append(speedup)
                    print(f"Distribution: {dist_query[0]}, Query: {dist_query[1]}, Baseline: {baseline_times[dist_query]}, Measure: {measure_times[dist_query]}, Speedup: {speedup}")
        x_labels = measures
        y_values = [gmean([speedup for dist in speedups for speedup in speedups[dist][measure]]) for measure in measures]

    elif mode == 'algorithm':
        baseline_algorithm = 'AGGLOMERATIVE'
        algorithms = ['AGGLOMERATIVE', 'GMM', 'DBSCAN', 'OPTICS']
        baseline_times = df[df['Algorithm'] == baseline_algorithm].groupby(['Distribution', 'Query'])['TotalTime'].mean()
        speedups = defaultdict(lambda: defaultdict(list))
        for algorithm in algorithms:
            algorithm_times = df[df['Algorithm'] == algorithm].groupby(['Distribution', 'Query'])['TotalTime'].mean()
            for dist_query in baseline_times.index:
                if dist_query in algorithm_times.index:
                    speedup = algorithm_times[dist_query] / baseline_times[dist_query]
                    speedups[dist_query[0]][algorithm].append(speedup)
                    print(f"Distribution: {dist_query[0]}, Query: {dist_query[1]}, Baseline: {baseline_times[dist_query]}, Algorithm: {algorithm_times[dist_query]}, Speedup: {speedup}")
        x_labels = algorithms
        y_values = [gmean([speedup for dist in speedups for speedup in speedups[dist][algorithm]]) for algorithm in algorithms]

    elif mode == 'layering':
        baseline_layering = 'DESCENDING'
        layerings = ['DESCENDING', 'ASCENDING', 'NO_SORT']
        baseline_times = df[df['Layering'] == baseline_layering].groupby(['Distribution', 'Query'])['TotalTime'].mean()
        speedups = defaultdict(lambda: defaultdict(list))
        for layering in layerings:
            layering_times = df[df['Layering'] == layering].groupby(['Distribution', 'Query'])['TotalTime'].mean()
            for dist_query in baseline_times.index:
                if dist_query in layering_times.index:
                    speedup = layering_times[dist_query] / baseline_times[dist_query]
                    speedups[dist_query[0]][layering].append(speedup)
                    print(f"Distribution: {dist_query[0]}, Query: {dist_query[1]}, Baseline: {baseline_times[dist_query]}, Layering: {layering_times[dist_query]}, Speedup: {speedup}")
        x_labels = layerings
        y_values = [gmean([speedup for dist in speedups for speedup in speedups[dist][layering]]) for layering in layerings]

    # Print per-distribution speedups in tabular format
    print("\nPer-distribution Speedups:")
    distributions = sorted(speedups.keys())
    queries = sorted(set(df['Query']))
    table_data = []
    header = [""] + queries
    table_data.append(header)
    for distribution in distributions:
        for key in speedups[distribution]:
            row = [f"{distribution} {key}"]
            row.extend([f"{speedup:.2f}" for speedup in speedups[distribution][key]])
            table_data.append(row)
    col_widths = [max(len(str(cell)) for cell in col) for col in zip(*table_data)]
    for row in table_data:
        print(" | ".join((str(cell).ljust(width) for cell, width in zip(row, col_widths))))

    return x_labels, y_values

def plot_bar_chart(x_labels, y_values, mode, output_file, custom_labels, figsize, sf_number, title_fontsize, label_fontsize, tick_fontsize):
    plt.figure(figsize=figsize)
    patterns = ['/', '\\', '|', '-', '+', 'x', 'o', 'O', '.', '*']
    bars = plt.bar(custom_labels, y_values, edgecolor='black', color='white')

    if mode == 'measure':
        representative = 'OURS'
    elif mode == 'algorithm':
        representative = 'AGGLOMERATIVE'
    elif mode == 'layering':
        representative = 'DESCENDING'

    for bar, label, pattern in zip(bars, x_labels, patterns):
        if label == representative.upper():
            bar.set_color('black')
            bar.set_hatch('')
        else:
            bar.set_hatch(pattern)
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width() / 2., height, f'{height:.2f}', ha='center', va='bottom', fontsize=tick_fontsize)

    # Adding SF label
    plt.text(0.95, 0.95, f'SF{sf_number}', ha='right', va='top', transform=plt.gca().transAxes, fontsize=label_fontsize)

    plt.xlabel(mode.capitalize(), fontsize=label_fontsize)
    plt.ylabel('Geometric Mean Speedup', fontsize=label_fontsize)
    plt.title(f'Geometric Mean Speedup by {mode.capitalize()}', fontsize=title_fontsize)
    plt.xticks(fontsize=tick_fontsize)
    plt.yticks(fontsize=tick_fontsize)
    plt.tight_layout()
    plt.savefig(output_file)

def main(folder, mode, sf_number):
    data = read_logs(folder)
    x_labels, y_values = calculate_geomean_speedups(data, mode)

    # Define custom labels for X-axis
    custom_labels_dict = {
        'measure': ['Ours', 'Overlap', 'Jaccard', 'Weighted Jaccard', 'Cosine', 'Dice'],
        'algorithm': ['Ours', 'GMM', 'DBSCAN', 'OPTICS'],
        'layering': ['Descending', 'Ascending', 'No Sort']
    }
    custom_labels = custom_labels_dict[mode]

    # Define default figsize and text properties
    figsize = (10, 6)  # width, height in inches
    title_fontsize = 14
    label_fontsize = 12
    tick_fontsize = 10

    output_file = f'geomean_speedup_by_{mode}.png'
    plot_bar_chart(x_labels, y_values, mode, output_file, custom_labels, figsize, sf_number, title_fontsize, label_fontsize, tick_fontsize)
    print(f"Bar chart saved as {output_file}")

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python script.py <folder_path> <mode> <sf_number>")
        sys.exit(1)

    folder = sys.argv[1]
    mode = sys.argv[2].lower()
    sf_number = sys.argv[3]
    if mode not in ['measure', 'algorithm', 'layering']:
        print("Invalid mode. Choose from 'measure', 'algorithm', or 'layering'.")
        sys.exit(1)

    main(folder, mode, sf_number)
