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

# File names per method
methods = {
    'SS': 'ss.csv',
    'US': 'us.csv',
    'SSRF': 'ssrf.csv'
}

# Collected data per method
join_data = {}
agg_data = {}
hop_counts = None  # We'll get this from the CSVs

# Load data
for method_key, filename in methods.items():
    file_path = os.path.join(folder_path, filename)
    df = pd.read_csv(file_path)

    if hop_counts is None:
        hop_counts = df['Hops'].tolist()

    join_data[method_key] = pd.to_numeric(df['JoinTime'], errors='coerce').tolist()
    agg_data[method_key] = pd.to_numeric(df['AggTime'], errors='coerce').tolist()

# Plotting
fig, ax = plt.subplots(figsize=(5, 3))
ax.set_xlabel("Number of Hops", fontsize=16)
ax.set_ylabel("Time (ms)", fontsize=16)
ax.set_xticks(hop_counts)
ax.tick_params(axis='x', labelsize=15)
ax.tick_params(axis='y', labelsize=13)
ax.set_ylim(bottom=-1000, top=80000)

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
          fontsize=12,
          ncol=2,
          title='Join = solid, Agg = dashed',
          title_fontsize=12,
          columnspacing=1,
          handlelength=2)

plt.tight_layout()

# Save output
output_path = os.path.join(folder_path, 'hop-time-lineplot.pdf')
plt.savefig(output_path, dpi=800, bbox_inches='tight')
plt.close(fig)

print(f"Line plot saved to: {output_path}")
