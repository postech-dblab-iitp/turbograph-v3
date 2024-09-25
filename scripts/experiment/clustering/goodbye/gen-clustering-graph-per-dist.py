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
        if filename.startswith('goodbye_zipf') and filename.endswith('.txt'):
            parts = filename.split('_')
            distribution = parts[2]
            query_number = parts[3]
            algorithm = parts[4]
            measure = parts[5]
            layering = '_'.join(parts[6:]).replace('.txt', '')
            total_time = extract_times(os.path.join(folder, filename))
            if total_time is not None:
                data.append((distribution, query_number, algorithm, measure, layering, total_time))
    return data

def calculate_geomean_speedups(df, mode):
    speedups = defaultdict(lambda: defaultdict(list))
    if mode == 'measure':
        baseline_measure = 'OURS'
        measures = ['OVERLAP', 'JACCARD', 'DICE', 'COSINE', 'WEIGHTEDJACCARD', 'OURS']
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
        baseline_times = df[df['Layering'] == baseline_layering].groupby(['Query'])['TotalTime'].mean()
        for layering in layerings:
            layering_times = df[df['Layering'] == layering].groupby(['Query'])['TotalTime'].mean()
            for query in baseline_times.index:
                if query in layering_times.index:
                    speedup = layering_times[query] / baseline_times[query]
                    speedups[query][layering].append(speedup)
        x_labels = layerings
        y_values = [gmean([speedup for query in speedups for speedup in speedups[query][layering]]) for layering in layerings]

    return x_labels, y_values

def plot_aggregate_bar_chart(distributions, data, mode, output_file, custom_labels, figsize, label_fontsize, tick_fontsize, bar_gap):
    plt.figure(figsize=figsize)

    # Define colors for each competitor
    colors = ['#FFFFFF', '#E0E0E0', '#C0C0C0', '#808080', '#404040', 'black']
    if mode == 'algorithm':
        colors = ['lightgray', 'gray', 'black']

    all_x_labels = []
    all_y_values = []
    bar_positions = []
    current_position = 0
    bar_width = 1  # Set bar width to 1 for no gap between bars

    for distribution in distributions:
        dist_data = data[data['Distribution'] == distribution]
        x_labels, y_values = calculate_geomean_speedups(dist_data, mode)
        all_x_labels.extend(x_labels)
        all_y_values.extend(y_values)

        # Append positions for bars within the current distribution without gaps
        bar_positions.extend(np.arange(current_position, current_position + len(x_labels)))
        midpoint = current_position + len(x_labels) / 2 - 0.5

        # Add gap only between distributions
        current_position += len(x_labels) + bar_gap

        # Place the distribution label in the center of the bars for this distribution
        if distribution == '0':
            plt.text(midpoint, -0.14, 'Uniform', ha='center', va='center', fontsize=label_fontsize)
        else:
            plt.text(midpoint, -0.14, f'Zipf-{distribution}', ha='center', va='center', fontsize=label_fontsize)

    bars = plt.bar(bar_positions, all_y_values, width=bar_width, color=[colors[i % len(colors)] for i in range(len(all_y_values))], edgecolor='black', linewidth=0.5)

    plt.ylabel('Normalized\nEnd-to-End Exec Time', fontsize=label_fontsize)
    plt.xticks([])
    plt.yticks(fontsize=tick_fontsize)
    plt.xlabel('Distributions', fontsize=22, labelpad=37)

    # Set the y-axis limit to [0, 1.2]
    if mode == 'measure':
        plt.ylim(0, 1.4)
        plt.yticks(np.arange(0, 1.401, step=0.2))
        # plt.legend(bars[:len(custom_labels)], custom_labels, loc='upper center', bbox_to_anchor=(0.5, 1.3), fontsize=16, ncol=len(custom_labels)/2)
        plt.legend(bars[:len(custom_labels)], custom_labels, loc='upper center', fontsize=20, ncol=len(custom_labels)/2, handletextpad=0.5, labelspacing=0.01, handlelength=1, borderpad=0.1, columnspacing=1, borderaxespad=0.1)
    else:
        plt.ylim(0, 2.0)
        plt.yticks(np.arange(0, 2.01, step=0.5))
        plt.legend(bars[:len(custom_labels)], custom_labels, loc='upper left', fontsize=19, ncol=len(custom_labels)/2, handletextpad=0.5, labelspacing=0.01, borderpad=0.2,  borderaxespad=0.1)

    plt.tight_layout()
    plt.savefig(output_file, bbox_inches='tight')

def main(base_folder, mode):
    scale_factors = ['sf1', 'sf10', 'sf100']
    all_data = []

    for sf in scale_factors:
        folder = os.path.join(base_folder, sf)
        if os.path.exists(folder):
            data = read_logs(folder)
            all_data.extend(data)

    df = pd.DataFrame(all_data, columns=['Distribution', 'Query', 'Algorithm', 'Measure', 'Layering', 'TotalTime'])

    distributions = sorted(df['Distribution'].unique())

    custom_labels_dict = {
        'measure': ['Overlap', 'Jaccard', 'Dice', 'Cosine', 'W-Jaccard', 'Ours'],
        'algorithm': ['GMMSchema', 'DBSCAN', 'Ours'],
        'layering': ['Descending', 'Ascending', 'No Sort']
    }
    custom_labels = custom_labels_dict[mode]

    figsize = (7, 5)
    label_fontsize = 22
    tick_fontsize = 19
    bar_gap = 1  # Adjustable gap between distributions

    output_file = f'aggregate_geomean_speedup_by_{mode}.pdf'
    plot_aggregate_bar_chart(distributions, df, mode, output_file, custom_labels, figsize, label_fontsize, tick_fontsize, bar_gap)
    print(f"Aggregate bar chart saved as {output_file}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script.py <base_folder> <mode>")
        sys.exit(1)

    base_folder = sys.argv[1]
    mode = sys.argv[2].lower()
    if mode not in ['measure', 'algorithm', 'layering']:
        print("Invalid mode. Choose from 'measure', 'algorithm', or 'layering'.")
        sys.exit(1)

    main(base_folder, mode)
