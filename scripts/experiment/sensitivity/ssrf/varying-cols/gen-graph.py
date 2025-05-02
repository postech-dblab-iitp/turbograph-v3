import os
import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import Patch

# Get parent folder from command-line argument
folder_path = sys.argv[1]

# Methods and their file name tags
methods = {
    'US': 'UNION',
    'SS': 'SS',
    'SSRF': 'SSRF'
}

# Store slowdown data per column count
slowdowns = {col: {'US': [], 'SS': []} for col in range(1, 6)}

# Process each column count (1 to 5)
for col in range(1, 6):
    # Load SSRF as baseline
    ssrf_file = os.path.join(folder_path, 'SSRF', f'format_{col}cols_SSRF.csv')
    df_ssrf = pd.read_csv(ssrf_file).set_index('QueryNumber')
    ssrf_exec = df_ssrf['QueryExecutionTime']

    for method_key in ['US', 'SS']:
        method_file = os.path.join(folder_path, method_key, f'format_{col}cols_{methods[method_key]}.csv')
        df_method = pd.read_csv(method_file).set_index('QueryNumber')
        method_exec = df_method['QueryExecutionTime']

        for query in df_method.index:
            if query in ssrf_exec:
                val_base = pd.to_numeric(ssrf_exec.get(query), errors='coerce')
                val_this = pd.to_numeric(method_exec.get(query), errors='coerce')
                if not np.isnan(val_base) and val_base > 0 and \
                   not np.isnan(val_this) and val_this > 0:
                    slowdown = val_this / val_base
                    slowdowns[col][method_key].append(slowdown)
                    
    # Print results
    for method_key in ['US', 'SS']:
        slowdowns[col][method_key] = [s for s in slowdowns[col][method_key] if s > 0]
        print(f"Slowdown ({method_key}) for {col} columns: {slowdowns[col][method_key]}")

# Prepare for plotting
fig, ax = plt.subplots(figsize=(5, 3))
ax.set_ylabel('Relative Slowdown', fontsize=16)
ax.set_xlabel('Number of Columns', fontsize=16, labelpad=10)  # ← label below x-axis
ax.set_ylim(bottom=10**(-0.1), top=3)
ax.set_yticks(np.arange(1, 3.5, 0.5))
ax.tick_params(axis='y', labelsize=14)

# X-axis layout
box_width = 0.4
spacing = 1.5
positions_us = []
positions_ss = []
xtick_positions = []
top_xtick_labels = []

x = 1
for col in range(1, 6):
    positions_us.append(x - 0.2)
    positions_ss.append(x + 0.2)
    xtick_positions.append(x)
    top_xtick_labels.append(str(col))
    x += spacing

# Plot boxplots
for i, col in enumerate(range(1, 6)):
    ax.boxplot(slowdowns[col]['US'], positions=[positions_us[i]], widths=box_width,
               patch_artist=True,
               boxprops=dict(facecolor='white', edgecolor='black'),
               medianprops=dict(color='black'),
               whiskerprops=dict(color='black'),
               capprops=dict(color='black'),
               flierprops=dict(markerfacecolor='black', markersize=5))
    
    ax.boxplot(slowdowns[col]['SS'], positions=[positions_ss[i]], widths=box_width,
               patch_artist=True,
               boxprops=dict(facecolor='black', edgecolor='black'),
               medianprops=dict(color='white'),
               whiskerprops=dict(color='black'),
               capprops=dict(color='black'),
               flierprops=dict(markerfacecolor='black', markersize=5))

# Reference line at y=1
ax.axhline(y=1, color='black', linestyle=':', linewidth=1.8)

# Dotted vertical lines between columns
for i in range(1, 5):  # 4 separators between 5 groups
    sep = xtick_positions[i - 1] + spacing / 2
    ax.axvline(x=sep, color='black', linestyle=':', linewidth=1)

# X-ticks: numbers 1~5
ax.set_xticks(xtick_positions)
ax.set_xticklabels(top_xtick_labels, fontsize=16)

# Legend at top
legend_elements = [
    Patch(facecolor='white', edgecolor='black', label='US'),
    Patch(facecolor='black', edgecolor='black', label='SS')
]
ax.legend(handles=legend_elements, loc='upper left',
          ncol=2, frameon=True, fontsize=15)

ax.grid(axis='y', linestyle='--', linewidth=0.5)
plt.tight_layout()

# Save figure
output_path = os.path.join(folder_path, 'ssrf-comparison.pdf')
plt.savefig(output_path, dpi=800, bbox_inches='tight')
plt.close(fig)

print(f"Slowdown comparison plot saved to: {output_path}")
