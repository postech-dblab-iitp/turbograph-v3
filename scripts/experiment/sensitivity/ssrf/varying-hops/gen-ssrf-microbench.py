import os
import sys
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

import matplotlib as mpl
mpl.rcParams['lines.linewidth'] = 2
mpl.rcParams['lines.markersize'] = 7

# Get input folder from command-line
folder_path = sys.argv[1]

# Method folders and filename tags
methods = {
    'SS': 'SS',
    'US': 'UNION',
    'SSRF': 'SSRF'
}

# Number of hops to process
num_hops = 5

# Collected data per method
hop_counts = list(range(1, num_hops + 1))
join_data = {method: [] for method in methods}
agg_data = {method: [] for method in methods}

# Load data
for method_key, file_tag in methods.items():
    for hop in hop_counts:
        file_path = os.path.join(folder_path, method_key, f'format_{hop}hops_{file_tag}.csv')
        df = pd.read_csv(file_path)
        join_time = pd.to_numeric(df['JoinTime'].iloc[0], errors='coerce')
        agg_time = pd.to_numeric(df['AggTime'].iloc[0], errors='coerce')
        join_data[method_key].append(join_time)
        agg_data[method_key].append(agg_time)

# Plotting
fig, ax = plt.subplots(figsize=(5, 3.5))
ax.set_xlabel("Number of Hops", fontsize=12)
ax.set_ylabel("Time (ms)", fontsize=12)
ax.set_xticks(hop_counts)

# Color-blind friendly palette
method_colors = {
    'SS': 'black',
    'US': 'gray',
    'SSRF': 'blue'
}

# Line styles
join_style = 'solid'
agg_style = 'dashed'
markers = {
    'SS': 'o',
    'US': 's',
    'SSRF': '^'
}

# Draw lines (Join and Agg)
for method in methods:
    ax.plot(hop_counts, join_data[method],
            label=f'{method} Join',
            color=method_colors[method],
            marker=markers[method],
            linestyle=join_style)
    
    ax.plot(hop_counts, agg_data[method],
            label=f'{method} Agg',
            color=method_colors[method],
            marker=markers[method],
            linestyle=agg_style)

# Grid
ax.grid(True, linestyle='--', linewidth=0.5)

# Custom grouped legend (manual)
legend_elements = []
for method in ['SS', 'US', 'SSRF']:
    color = method_colors[method]
    marker = markers[method]
    legend_elements.append(Line2D([0], [0], color=color, marker=marker,
                                  linestyle='solid', label=f'{method} Join'))
    legend_elements.append(Line2D([0], [0], color=color, marker=marker,
                                  linestyle='dashed', label=f'{method} Agg'))

ax.legend(handles=legend_elements,
          loc='upper left',
          fontsize=10,
          ncol=2,
          title='Join = solid, Agg = dashed',
          title_fontsize=10,
          columnspacing=1.2,
          handlelength=3)

plt.tight_layout()

# Save output
output_path = os.path.join(folder_path, 'hop-time-lineplot.pdf')
plt.savefig(output_path, dpi=800, bbox_inches='tight')
plt.close(fig)

print(f"Line plot saved to: {output_path}")
