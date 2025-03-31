import os
import sys
import pandas as pd
import numpy as np
from scipy.stats import gmean
import matplotlib.pyplot as plt
from matplotlib.patches import Patch

# Get folder from command-line
folder_path = sys.argv[1]

# File names and their labels
method_files = {
    'SA': 'sa.csv',
    'MA': 'ma.csv',
    'GMMSchema': 'gmm.csv'
}
ours_file = 'ours.csv'

# Query category ranges
categories = {
    'Projection': range(2, 31),
    'EQ Predicate': range(31, 51),
    'Range Predicate': range(51, 71),
    'Agg': range(71, 101)
}

# Load ours.csv
ours_df = pd.read_csv(os.path.join(folder_path, ours_file))

# Helper: get category from query ID like "Q32"
def get_category(query):
    try:
        qnum = int(query[1:])
        for cat, rng in categories.items():
            if qnum in rng:
                return cat
    except:
        pass
    return None

# Compute compile and execution slowdowns
def compute_compile_exec_slowdown(method_label, method_csv):
    df = pd.read_csv(os.path.join(folder_path, method_csv)).set_index('QueryNumber')
    compile_slowdowns = []
    exec_slowdowns = []

    ours_compile = ours_df.set_index('QueryNumber')['CompileTime']
    ours_exec = ours_df.set_index('QueryNumber')['QueryExecutionTime']

    print(f"\n[{method_label}] Slowdown values vs 'ours':")
    for query in df.index:
        try:
            val_ours_compile = pd.to_numeric(ours_compile.get(query), errors='coerce')
            val_ours_exec = pd.to_numeric(ours_exec.get(query), errors='coerce')
            val_compile = pd.to_numeric(df.at[query, 'CompileTime'], errors='coerce')
            val_exec = pd.to_numeric(df.at[query, 'QueryExecutionTime'], errors='coerce')

            if not np.isnan(val_ours_compile) and val_ours_compile > 0:
                if not np.isnan(val_compile) and val_compile > 0:
                    compile_slowdown = val_compile / val_ours_compile
                    compile_slowdowns.append(compile_slowdown)
                    print(f"  [Compile] {query}: {compile_slowdown:.2f}x")

            if not np.isnan(val_ours_exec) and val_ours_exec > 0:
                if not np.isnan(val_exec) and val_exec > 0:
                    exec_slowdown = val_exec / val_ours_exec
                    exec_slowdowns.append(exec_slowdown)
                    print(f"  [Exec]    {query}: {exec_slowdown:.2f}x")
        except:
            continue

    if compile_slowdowns:
        print(f"  [{method_label}] Compile Geomean: {gmean(compile_slowdowns):.2f}x")
    else:
        print(f"  [{method_label}] Compile Geomean: N/A")

    if exec_slowdowns:
        print(f"  [{method_label}] Execution Geomean: {gmean(exec_slowdowns):.2f}x")
    else:
        print(f"  [{method_label}] Execution Geomean: N/A")

    return compile_slowdowns, exec_slowdowns

# Collect all slowdowns
compile_data = []
exec_data = []
labels = []

for label, filename in method_files.items():
    compile_slow, exec_slow = compute_compile_exec_slowdown(label, filename)
    compile_data.append(compile_slow)
    exec_data.append(exec_slow)
    labels.append(label)

# Plotting
fig, ax = plt.subplots(figsize=(10, 5))
ax.set_yscale('log')
ax.set_ylabel('Relative Geomean Performance Slowdown (log scale)', fontsize=12)
ax.set_ylim(bottom=1e-1, top=1e2)

# Boxplot placement strategy
# For each group: compile at X=G-0.2, exec at X=G+0.2
# Labels at X=G
num_labels = len(labels)
x_ticks = []
compile_positions = []
exec_positions = []

for i in range(num_labels):
    group_x = i * 2 + 1  # center of group
    compile_positions.append(group_x - 0.3)
    exec_positions.append(group_x + 0.3)
    x_ticks.append(group_x)

# Draw boxplots
for i in range(num_labels):
    ax.boxplot(compile_data[i], positions=[compile_positions[i]], widths=0.4,
               patch_artist=True,
               boxprops=dict(facecolor='white', edgecolor='black'),
               medianprops=dict(color='black'),
               whiskerprops=dict(color='black'),
               capprops=dict(color='black'),
               flierprops=dict(markerfacecolor='black', markersize=3))

    ax.boxplot(exec_data[i], positions=[exec_positions[i]], widths=0.4,
               patch_artist=True,
               boxprops=dict(facecolor='gray', edgecolor='black'),
               medianprops=dict(color='black'),
               whiskerprops=dict(color='black'),
               capprops=dict(color='black'),
               flierprops=dict(markerfacecolor='black', markersize=3))

# Draw vertical dividers between groups
for i in range(1, num_labels):
    divider_x = i * 2
    ax.axvline(x=divider_x, color='black', linestyle='--', linewidth=0.7)

# Add horizontal reference at slowdown = 1
ax.axhline(y=1, color='black', linestyle=':', linewidth=1.8)

# Set X-axis labels
ax.set_xticks(x_ticks)
ax.set_xticklabels(labels, fontsize=10)

# Legend (top row)
legend_elements = [
    Patch(facecolor='white', edgecolor='black', label='Compile Time'),
    Patch(facecolor='gray', edgecolor='black', label='Execution Time')
]
ax.legend(handles=legend_elements, loc='upper center', bbox_to_anchor=(0.5, 1.15),
          ncol=2, frameon=False, fontsize=13)

ax.grid(axis='y', linestyle='--', linewidth=0.5)
plt.tight_layout()

# Save
output_path = os.path.join(folder_path, 'slowdown_compile_exec_boxplot.png')
plt.savefig(output_path, dpi=800, bbox_inches='tight')
plt.close(fig)

print(f"\nSlowdown boxplot saved to: {output_path}")