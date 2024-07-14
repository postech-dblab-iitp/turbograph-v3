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
        if filename.startswith('goodbye_zipf'):
            parts = filename.split('_')
            distribution = parts[2]
            query_number = parts[3]
            algorithm = parts[4]
            measure = parts[5]
            layering = parts[6]
            total_time = extract_times(os.path.join(folder, filename))
            if total_time is not None:
                data.append((distribution, query_number, algorithm, measure, layering, total_time))
    return data

def calculate_geomean_speedups(data, mode):
    df = pd.DataFrame(data, columns=['Distribution', 'Query', 'Algorithm', 'Measure', 'Layering', 'TotalTime'])

    if mode == 'measure':
        baseline_measure = 'OURS'
        measures = ['OURS', 'OVERLAP', 'JACCARD', 'WEIGHTEDJACCARD', 'COSINE', 'DICE']
        baseline_times = df[df['Measure'] == baseline_measure].groupby(['Distribution', 'Query'])['TotalTime'].mean()
        speedups = {}
        for measure in measures:
            measure_times = df[df['Measure'] == measure].groupby(['Distribution', 'Query'])['TotalTime'].mean()
            speedups[measure] = gmean(baseline_times / measure_times)
        x_labels = measures
        y_values = [speedups[measure] for measure in measures]

    elif mode == 'algorithm':
        baseline_algorithm = 'AGGLOMERATIVE'
        algorithms = ['AGGLOMERATIVE', 'GMM', 'DBSCAN', 'OPTICS']
        baseline_times = df[df['Algorithm'] == baseline_algorithm].groupby(['Distribution', 'Query'])['TotalTime'].mean()
        speedups = {}
        for algorithm in algorithms:
            algorithm_times = df[df['Algorithm'] == algorithm].groupby(['Distribution', 'Query'])['TotalTime'].mean()
            speedups[algorithm] = gmean(baseline_times / algorithm_times)
        x_labels = algorithms
        y_values = [speedups[algorithm] for algorithm in algorithms]

    elif mode == 'layering':
        baseline_layering = 'DESCENDING'
        layerings = ['DESCENDING', 'ASCENDING', 'NOORDER']
        baseline_times = df[df['Layering'] == baseline_layering].groupby(['Distribution', 'Query'])['TotalTime'].mean()
        speedups = {}
        for layering in layerings:
            layering_times = df[df['Layering'] == layering].groupby(['Distribution', 'Query'])['TotalTime'].mean()
            speedups[layering] = gmean(baseline_times / layering_times)
        x_labels = layerings
        y_values = [speedups[layering] for layering in layerings]

    return x_labels, y_values

def plot_bar_chart(x_labels, y_values, mode, output_file, custom_labels, figsize):
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
    
    plt.xlabel(mode.capitalize())
    plt.ylabel('Geometric Mean Speedup')
    plt.title(f'Geometric Mean Speedup by {mode.capitalize()}')
    plt.tight_layout()
    plt.savefig(output_file)

def main(folder, mode):
    data = read_logs(folder)
    x_labels, y_values = calculate_geomean_speedups(data, mode)
    
    # Define custom labels for X-axis
    custom_labels_dict = {
        'measure': ['Ours', 'Overlap', 'Jaccard', 'Weighted Jaccard', 'Cosine', 'Dice'],
        'algorithm': ['Ours', 'GMM', 'DBSCAN', 'OPTICS'],
        'layering': ['Descending', 'Ascending', 'No Order']
    }
    custom_labels = custom_labels_dict[mode]
    
    # Define default figsize
    figsize = (5, 6)  # width, height in inches
    
    output_file = f'geomean_speedup_by_{mode}.png'
    plot_bar_chart(x_labels, y_values, mode, output_file, custom_labels, figsize)
    print(f"Bar chart saved as {output_file}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script.py <folder_path> <mode>")
        sys.exit(1)
    
    folder = sys.argv[1]
    mode = sys.argv[2].lower()
    if mode not in ['measure', 'algorithm', 'layering']:
        print("Invalid mode. Choose from 'measure', 'algorithm', or 'layering'.")
        sys.exit(1)
    
    main(folder, mode)
