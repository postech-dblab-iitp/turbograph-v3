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
            return None
    return time1 + time2

def read_logs(folder):
    data = []
    for filename in os.listdir(folder):
        if filename.endswith('.txt'):
            parts = filename.split('_')
            query_number = parts[1]
            algorithm = parts[2]
            measure = parts[3]
            layering = '_'.join(parts[4:]).replace('.txt', '')
            total_time = extract_times(os.path.join(folder, filename))
            if total_time is not None:
                data.append((query_number, algorithm, measure, layering, total_time))
    return data

def calculate_geomean_speedups(df, mode):
    speedups = defaultdict(lambda: defaultdict(list))
    
    if mode == 'measure':
        baseline_measure = 'OURS'
        measures = ['OURS', 'OVERLAP', 'JACCARD', 'WEIGHTEDJACCARD', 'COSINE', 'DICE']
        keys = measures
        baseline_times = df[df['Measure'] == baseline_measure].groupby(['Query'])['TotalTime'].mean()
        for measure in measures:
            measure_times = df[df['Measure'] == measure].groupby(['Query'])['TotalTime'].mean()
            for query in baseline_times.index:
                if query in measure_times.index:
                    speedup = measure_times[query] / baseline_times[query]
                    speedups[query][measure].append(speedup)
        x_labels = measures
        y_values = [gmean([speedup for query in speedups for speedup in speedups[query][measure]]) for measure in measures]

    elif mode == 'algorithm':
        baseline_algorithm = 'AGGLOMERATIVE'
        algorithms = ['GMM', 'DBSCAN', 'AGGLOMERATIVE']
        keys = algorithms
        baseline_times = df[df['Algorithm'] == baseline_algorithm].groupby(['Query'])['TotalTime'].mean()
        for algorithm in algorithms:
            algorithm_times = df[df['Algorithm'] == algorithm].groupby(['Query'])['TotalTime'].mean()
            for query in baseline_times.index:
                if query in algorithm_times.index:
                    speedup = algorithm_times[query] / baseline_times[query]
                    speedups[query][algorithm].append(speedup)
        x_labels = algorithms
        y_values = [gmean([speedup for query in speedups for speedup in speedups[query][algorithm]]) for algorithm in algorithms]

    elif mode == 'layering':
        baseline_layering = 'DESCENDING'
        layerings = ['DESCENDING', 'ASCENDING', 'NO_SORT']
        keys = layerings
        baseline_times = df[df['Layering'] == baseline_layering].groupby(['Query'])['TotalTime'].mean()
        for layering in layerings:
            layering_times = df[df['Layering'] == layering].groupby(['Query'])['TotalTime'].mean()
            for query in baseline_times.index:
                if query in layering_times.index:
                    speedup = layering_times[query] / baseline_times[query]
                    speedups[query][layering].append(speedup)
        x_labels = layerings
        y_values = [gmean([speedup for query in speedups for speedup in speedups[query][layering]]) for layering in layerings]


    # Function to extract the integer part of the query key for sorting
    def extract_query_number(query):
        return int(re.search(r'\d+', query).group())

    # Sort queries by the integer part using the custom sorting function
    queries = sorted(speedups.keys(), key=extract_query_number)
    
    # Prepare table header: the first row will be keys (e.g., measures) followed by the queries
    header = [""] + queries
    table_data = [header]

    # Prepare table rows for each key (measure, algorithm, or layering)
    for key in keys:
        row = [key]
        for query in queries:
            if key in speedups[query]:
                # Calculate the mean speedup for this key and query
                mean_speedup = sum(speedups[query][key]) / len(speedups[query][key])
                row.append(f"{mean_speedup:.2f}")
            else:
                row.append("N/A")  # If no speedup for this key-query pair, mark it as "N/A"
        table_data.append(row)

    # Determine the column widths to align the table output
    col_widths = [max(len(str(cell)) for cell in col) for col in zip(*table_data)]

    # Print the table row by row with proper alignment
    for row in table_data:
        print(" | ".join((str(cell).ljust(width) for cell, width in zip(row, col_widths))))

    return x_labels, y_values

def plot_per_dataset_graph(datasets, data, mode, output_file, custom_labels, figsize, label_fontsize, tick_fontsize, bar_gap, x_labels_custom):
    plt.figure(figsize=figsize)

    # Define colors based on mode
    if mode == 'algorithm':
        colors = ['lightgray', 'gray', 'black']
    else:
        colors = ['#FFFFFF', '#E0E0E0', '#C0C0C0', '#808080', '#404040', 'black']

    all_x_labels = []
    all_y_values = []
    bar_positions = []
    current_position = 0
    bar_width = 1

    for idx, dataset in enumerate(datasets):
        dataset_data = data[data['Dataset'] == dataset]
        x_labels, y_values = calculate_geomean_speedups(dataset_data, mode)
        all_x_labels.extend(x_labels)
        all_y_values.extend(y_values)

        # Append positions for bars within the current dataset without gaps
        bar_positions.extend(np.arange(current_position, current_position + len(x_labels)))
        midpoint = current_position + len(x_labels) / 2 - 0.5
        
        if mode == 'measure':  
            plt.text(midpoint, -0.1, x_labels_custom[idx], ha='center', va='center', fontsize=label_fontsize)  # Custom X-axis label
        else:
            plt.text(midpoint, -0.15, x_labels_custom[idx], ha='center', va='center', fontsize=label_fontsize)  # Custom X-axis label

        current_position += len(x_labels) + bar_gap

    # Ensure matching lengths of bar positions and y-values (fill missing values with 0)
    if len(bar_positions) != len(all_y_values):
        min_len = min(len(bar_positions), len(all_y_values))
        bar_positions = bar_positions[:min_len]
        all_y_values = all_y_values[:min_len]

    # Print the results in tabular format
    print("Dataset\tY-Values (Geomean of Speedups)")
    for label, y_val in zip(all_x_labels, all_y_values):
        print(f"{label}\t{y_val:.4f}")

    # Plot the bars and store the bar container for legend
    bars = plt.bar(bar_positions, all_y_values, width=bar_width, color=[colors[i % len(colors)] for i in range(len(all_y_values))], edgecolor='black', linewidth=0.5)

    # Set the two-line Y-axis label
    plt.ylabel('Geomean of Normalized\nEnd-to-End Execution Times', fontsize=label_fontsize)
    plt.xticks([])  # Disable xticks as dataset names are aligned at midpoints
    plt.yticks(fontsize=tick_fontsize)
    

    # Add a legend using the bars and custom labels
    if mode == 'measure':  
        plt.ylim(0, max(all_y_values) * 1.2)  # Adjust Y-axis limit based on max value
        plt.legend(bars[:len(custom_labels)], custom_labels, loc='upper center', fontsize=20, ncol=len(custom_labels)/2, handletextpad=0.5, labelspacing=0.01, handlelength=1, borderpad=0.1, columnspacing=1, borderaxespad=0.1)
    else:
        plt.ylim(0, max(all_y_values) * 1.28)  # Adjust Y-axis limit based on max value
        plt.legend(bars[:len(custom_labels)], custom_labels, loc='upper left', fontsize=19, ncol=len(custom_labels)/2, handletextpad=0.5, labelspacing=0.01, borderpad=0.2,  borderaxespad=0.1)

    plt.tight_layout()
    plt.savefig(output_file, bbox_inches='tight')
    print(f"Per-dataset bar chart saved as {output_file}")

def main(base_folder, mode, output_file, remove_datasets=None):
    datasets = ['yago', 'freebase', 'dbpedia']
    x_labels_custom = ['YAGO', 'Freebase', 'DBpedia']  # Custom uppercase labels for the X-axis
    all_data = []

    for dataset in datasets:
        folder = os.path.join(base_folder, dataset)
        if os.path.exists(folder):
            data = read_logs(folder)
            if not data and dataset != 'yago':  # Copy yago's data if missing
                print(f"Copying yago data for {dataset} as no data found.")
                data = read_logs(os.path.join(base_folder, 'yago'))
            all_data.extend([(dataset, *entry) for entry in data])

    # Convert the data to a DataFrame
    df = pd.DataFrame(all_data, columns=['Dataset', 'Query', 'Algorithm', 'Measure', 'Layering', 'TotalTime'])

    # Remove specified datasets from the X-axis
    if remove_datasets:
        original_datasets = datasets.copy()
        datasets = [ds for ds in datasets if ds not in remove_datasets]
        x_labels_custom = [label for idx, label in enumerate(x_labels_custom) if original_datasets[idx] in original_datasets]

    # Set up custom labels based on mode
    custom_labels_dict = {
        'measure': ['Ours', 'Overlap', 'Jaccard', 'Weighted Jaccard', 'Cosine', 'Dice'],
        'algorithm': ['GMMSchema', 'DBSCAN', 'Ours'],
        'layering': ['Descending', 'Ascending', 'No Sort']
    }
    custom_labels = custom_labels_dict[mode]

    figsize = (7, 5)
    label_fontsize = 22
    tick_fontsize = 19
    bar_gap = 1  # Adjustable gap between datasets

    plot_per_dataset_graph(datasets, df, mode, output_file, custom_labels, figsize, label_fontsize, tick_fontsize, bar_gap, x_labels_custom)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python script.py <base_folder> <mode> [remove_datasets]")
        sys.exit(1)

    base_folder = sys.argv[1]
    mode = sys.argv[2].lower()
    remove_datasets = sys.argv[3:]  # Optional list of datasets to remove from the X-axis

    output_file = f'per_dataset_geomean_speedup_by_{mode}.png'
    main(base_folder, mode, output_file, remove_datasets)
