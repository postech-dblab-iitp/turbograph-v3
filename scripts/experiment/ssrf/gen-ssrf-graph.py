import os
import sys
import re
import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import gmean

def extract_time_from_file(file_path):
    with open(file_path, 'r') as file:
        content = file.read()
    match = re.search(r'Average Query Execution Time: (\d+\.?\d*) ms', content)
    if match:
        return float(match.group(1))
    else:
        raise ValueError(f"Time not found in file {file_path}")

def calculate_slowdowns(folder_path, queries):
    slowdowns = {q: [] for q in queries}
    for query in queries:
        ssrf_times = []
        union_times = []
        for distribution in range(3):
            ssrf_file = os.path.join(folder_path, f"goodbye_zipf_{distribution}_SSRF_Q{query}.txt")
            union_file = os.path.join(folder_path, f"goodbye_zipf_{distribution}_UNION_Q{query}.txt")
            ssrf_time = extract_time_from_file(ssrf_file)
            union_time = extract_time_from_file(union_file)
            ssrf_times.append(ssrf_time)
            union_times.append(union_time)
        
        for ssrf_time, union_time in zip(ssrf_times, union_times):
            slowdown = union_time / ssrf_time
            slowdowns[query].append(slowdown)
    
    return slowdowns

def main(folder_path, scale_factor):
    queries = [1, 2, 6]  # Hard-coded queries
    slowdowns = calculate_slowdowns(folder_path, queries)

    # Print the slowdowns for debugging
    print("Slowdowns for each query and distribution:")
    for query in queries:
        print(f"Q{query}: ", end="")
        for distribution_slowdown in slowdowns[query]:
            print(f"{distribution_slowdown:.2f} ", end="")
        print()

    # Calculate geomean of slowdowns
    geomean_slowdowns = {query: gmean(slowdowns[query]) for query in queries}

    # Plotting
    formats = ['SSRF', 'US']
    y_values = [1, np.mean(list(geomean_slowdowns.values()))]  # SSRF is the baseline (1), UNION is geomean slowdown
    colors = ['black', 'gray']  # Configurable colors
    font_size = 12  # Configurable font size
    chart_size = (4, 5)  # Configurable chart size
    sf_text_location = (-0.4, max(y_values) + 0.01)  # Configurable SF text location

    plt.figure(figsize=chart_size)
    bars = plt.bar(formats, y_values, color=colors)
    
    # Add text on top of each bar
    for bar, value in zip(bars, y_values):
        plt.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.05, f'{value:.2f}', ha='center', va='bottom', fontsize=font_size)
    
    plt.text(sf_text_location[0], sf_text_location[1], f'SF{scale_factor}', fontsize=font_size, ha='left')
    plt.xlabel('Format', fontsize=font_size)
    plt.xticks(fontsize=font_size)
    plt.yticks(fontsize=font_size)
    plt.ylim(0, max(y_values) + 0.2)  # Adjust y-limits to provide more space

    # Save the chart as PNG
    output_file = 'bar_chart.png'
    plt.savefig(output_file, bbox_inches='tight')
    print(f"Bar chart saved as {output_file}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script.py <folder_path> <scale_factor>")
        sys.exit(1)
    
    folder_path = sys.argv[1]
    scale_factor = int(sys.argv[2])
    if scale_factor not in [1, 10, 100]:
        print("Scale factor must be 1, 10, or 100")
        sys.exit(1)
    
    main(folder_path, scale_factor)
