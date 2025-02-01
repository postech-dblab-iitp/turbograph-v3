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
        if filename.startswith('yago') and filename.endswith('.txt'):
            parts = filename.split('_')
            query_number = parts[1][1:]  # Strip 'Q' from the query number
            algorithm = parts[2]
            measure = parts[3]
            layering = parts[4].replace('.txt', '')
            total_time = extract_times(os.path.join(folder, filename))
            if total_time is not None:
                data.append((query_number, algorithm, measure, layering, total_time))
    return data

def calculate_relative_times(df, mode):
    relative_times = defaultdict(lambda: defaultdict(list))

    if mode == 'measure':
        baseline_measure = 'OURS'
        measures = ['OVERLAP', 'JACCARD', 'DICE', 'COSINE', 'WEIGHTEDJACCARD']
        baseline_times = df[df['Measure'] == baseline_measure].groupby(['Query'])['TotalTime'].mean()
        for measure in measures:
            measure_times = df[df['Measure'] == measure].groupby(['Query'])['TotalTime'].mean()
            for query in baseline_times.index:
                if query in measure_times.index:
                    relative_time = measure_times[query] / baseline_times[query]
                    relative_times[measure][query] = relative_time

    elif mode == 'algorithm':
        baseline_algorithm = 'AGGLOMERATIVE'
        algorithms = ['GMM', 'DBSCAN']
        baseline_times = df[df['Algorithm'] == baseline_algorithm].groupby(['Query'])['TotalTime'].mean()
        for algorithm in algorithms:
            algorithm_times = df[df['Algorithm'] == algorithm].groupby(['Query'])['TotalTime'].mean()
            for query in baseline_times.index:
                if query in algorithm_times.index:
                    relative_time = algorithm_times[query] / baseline_times[query]
                    relative_times[algorithm][query] = relative_time

    elif mode == 'layering':
        baseline_layering = 'DESCENDING'
        layerings = ['ASCENDING', 'NO_SORT']
        baseline_times = df[df['Layering'] == baseline_layering].groupby(['Query'])['TotalTime'].mean()
        for layering in layerings:
            layering_times = df[df['Layering'] == layering].groupby(['Query'])['TotalTime'].mean()
            for query in baseline_times.index:
                if query in layering_times.index:
                    relative_time = layering_times[layering] / baseline_times[query]
                    relative_times[layering][query] = relative_time

    # Print per-query relative times in tabular format
    print("\nPer-query Relative Execution Times:")
    queries = sorted(set(df['Query']))
    table_data = []
    header = [""] + queries
    table_data.append(header)
    for key in relative_times:
        row = [key]
        row.extend([f"{relative_times[key][query]:.2f}" if query in relative_times[key] else "N/A" for query in queries])
        table_data.append(row)

    col_widths = [max(len(str(cell)) for cell in col) for col in zip(*table_data)]
    for row in table_data:
        print(" | ".join((str(cell).ljust(width) for cell, width in zip(row, col_widths))))

    return relative_times

def plot_boxplot(relative_times, mode, output_file, custom_labels, figsize, label_fontsize, tick_fontsize):
    plt.figure(figsize=figsize)

    # Define colors for each competitor
    colors = ['#FFFFFF', '#E0E0E0', '#C0C0C0', '#808080', '#404040', 'black']
    if mode == 'algorithm':
        colors = ['lightgray', 'gray']

    # Prepare data for box plot based on index, assuming the order of data in relative_times
    data = [list(relative_times[key].values()) for key in relative_times]

    # Print the data to ensure we are passing valid data to the plot
    print("\nData used for box plot:")
    for label, values in zip(custom_labels, data):
        print(f"{label}: {values}")

    # Create the box plot
    box = plt.boxplot(data, patch_artist=True, showmeans=True)  # showmeans=True to ensure means are visible

    # Set the color for each box
    for patch, color in zip(box['boxes'], colors):
        patch.set_facecolor(color)

    # Set a fixed Y-axis range to ensure all data is visible
    plt.ylim(0, 10)  # You can adjust this based on the data range

    plt.ylabel('Relative Execution Time', fontsize=label_fontsize)
    plt.yticks(fontsize=tick_fontsize)

    # Create custom legend
    legend_patches = [plt.Line2D([0], [0], color=color, lw=4) for color in colors[:len(custom_labels)]]
    plt.legend(legend_patches, custom_labels, loc='upper center', fontsize=20, ncol=len(custom_labels)//2, handletextpad=0.5, labelspacing=0.01, handlelength=1, borderpad=0.1, columnspacing=1, borderaxespad=0.1)

    # Remove x-ticks and x-labels
    plt.xticks([])

    plt.tight_layout()
    plt.savefig(output_file, bbox_inches='tight')
    plt.show()  # Show the plot after saving

def main(folder, mode):
    all_data = []

    if os.path.exists(folder):
        data = read_logs(folder)
        all_data.extend(data)

    df = pd.DataFrame(all_data, columns=['Query', 'Algorithm', 'Measure', 'Layering', 'TotalTime'])

    custom_labels_dict = {
        'measure': ['Overlap', 'Jaccard', 'Dice', 'Cosine', 'W-Jaccard'],
        'algorithm': ['GMM', 'DBSCAN'],
        'layering': ['Ascending', 'No Sort']
    }
    custom_labels = custom_labels_dict[mode]

    figsize = (9, 5)
    label_fontsize = 22
    tick_fontsize = 19

    output_file = f'{mode}_relative_execution_time_boxplot.png'
    relative_times = calculate_relative_times(df, mode)
    plot_boxplot(relative_times, mode, output_file, custom_labels, figsize, label_fontsize, tick_fontsize)
    print(f"Box plot saved as {output_file}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script.py <folder> <mode>")
        sys.exit(1)

    folder = sys.argv[1]
    mode = sys.argv[2].lower()
    if mode not in ['measure', 'algorithm', 'layering']:
        print("Invalid mode. Choose from 'measure', 'algorithm', or 'layering'.")
        sys.exit(1)

    main(folder, mode)
