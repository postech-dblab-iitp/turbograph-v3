import os
import re
import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import gmean
import sys

def extract_time_from_file(file_path):
    if not os.path.exists(file_path):
        return None
    with open(file_path, 'r') as file:
        content = file.read()
    match = re.search(r'Average Query Execution Time: (\d+\.?\d*) ms', content)
    if match:
        return float(match.group(1))
    else:
        raise ValueError(f"Time not found in file {file_path}")

def calculate_slowdowns(folder_path, queries, scale_factors):
    slowdowns = {dist: {"US": [], "SS": [], "SSRF": []} for dist in range(3)}
    for scale_factor in scale_factors:
        scale_folder = os.path.join(folder_path, f'sf{scale_factor}')
        for distribution in range(3):
            ssrf_times = {query: [] for query in queries}
            ss_times = {query: [] for query in queries}
            union_times = {query: [] for query in queries}
            
            for query in queries:
                ssrf_file = os.path.join(scale_folder, f"goodbye_zipf_{distribution}_SSRF_Q{query}.txt")
                ss_file = os.path.join(scale_folder, f"goodbye_zipf_{distribution}_SS_Q{query}.txt")
                union_file = os.path.join(scale_folder, f"goodbye_zipf_{distribution}_UNION_Q{query}.txt")
                
                ssrf_time = extract_time_from_file(ssrf_file)
                ss_time = extract_time_from_file(ss_file)
                union_time = extract_time_from_file(union_file)

                if ssrf_time is None or union_time is None:
                    continue

                ssrf_times[query].append(ssrf_time)
                union_times[query].append(union_time)
                ss_times[query].append(ss_time if ss_time is not None else union_time)
            
            for query in queries:
                for ssrf_time, ss_time, union_time in zip(ssrf_times[query], ss_times[query], union_times[query]):
                    slowdown_us = union_time / ssrf_time
                    slowdown_ss = ss_time / ssrf_time
                    slowdowns[distribution]["US"].append(slowdown_us)
                    slowdowns[distribution]["SS"].append(slowdown_ss)
                    slowdowns[distribution]["SSRF"].append(1.0)  # SSRF is the baseline
                    
                    print(f"Scale Factor: {scale_factor}, Distribution: {distribution}, Query: {query}, "
                          f"SSRF Time: {ssrf_time}, SS Time: {ss_time}, UNION Time: {union_time}, "
                          f"Slowdown US: {slowdown_us}, Slowdown SS: {slowdown_ss}")
        
    return slowdowns

def main(folder_path):
    queries = [1, 2, 6]  # Hard-coded queries
    scale_factors = [1, 10, 100]  # Hard-coded scale factors
    slowdowns = calculate_slowdowns(folder_path, queries, scale_factors)

    # Calculate geomean of slowdowns for each distribution
    geomean_slowdowns = {dist: {format_type: gmean(slowdowns[dist][format_type]) if slowdowns[dist][format_type] else 1.0 for format_type in slowdowns[dist]} for dist in range(3)}

    # Plotting
    formats = ['US', 'SS', 'SSRF']  # Configurable order of formats
    colors = ['#FFFFFF', '#808080', 'black']  # Configurable colors
    border_color = 'black'  # Configurable border color
    border_width = 0.5  # Configurable border width
    tick_fontsize = 19
    chart_size = (7, 5)  # Configurable chart size

    plt.figure(figsize=chart_size)
    x_labels = ['Uniform', 'Zipf-1', 'Zipf-2']
    x = np.arange(len(x_labels))

    for i, format_type in enumerate(formats):
        y_values = [geomean_slowdowns[dist][format_type] for dist in range(3)]
        plt.bar(x + i * 0.25, y_values, width=0.25, label=format_type, color=colors[i], edgecolor=border_color, linewidth=border_width)

    plt.xticks(x + 0.25, x_labels, fontsize=22)
    plt.ylim(0, 6)
    plt.yticks(np.arange(0, 6.01, step=1))
    plt.yticks(fontsize=tick_fontsize)
    plt.ylabel('Normalized\nEnd-to-End Exec Time', fontsize=22)
    plt.legend(fontsize=20, ncol=3, loc='upper left', borderaxespad=0.1, columnspacing=1)
    plt.xlabel('Distributions', fontsize=22, labelpad=9)
    plt.tick_params(axis='x', which='both', bottom=False, top=False)
    plt.tight_layout()

    # Save the chart as PNG
    output_file = 'ssrf_per_distribution.pdf'
    plt.savefig(output_file, bbox_inches='tight')
    print(f"Bar chart saved as {output_file}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <folder_path>")
        sys.exit(1)
    
    folder_path = sys.argv[1]
    main(folder_path)
