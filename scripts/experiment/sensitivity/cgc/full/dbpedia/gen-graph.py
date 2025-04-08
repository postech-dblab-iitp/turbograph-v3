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

# Load ours.csv
ours_df = pd.read_csv(os.path.join(folder_path, ours_file)).set_index('QueryNumber')

# Compute compile and execution slowdowns
def compute_slowdown(method_label, method_csv):
    df = pd.read_csv(os.path.join(folder_path, method_csv)).set_index('QueryNumber')
    ours_compile = ours_df['CompileTime']
    ours_exec = ours_df['QueryExecutionTime']

    compile_vals = []
    exec_vals = []

    print(f"\n[{method_label}] Slowdowns for Q1–Q20:")
    for query in df.index:
        try:
            qnum = int(query[1:])
            if not (1 <= qnum <= 20):
                continue

            val_ours_compile = pd.to_numeric(ours_compile.get(query), errors='coerce')
            val_ours_exec = pd.to_numeric(ours_exec.get(query), errors='coerce')
            val_compile = pd.to_numeric(df.at[query, 'CompileTime'], errors='coerce')
            val_exec = pd.to_numeric(df.at[query, 'QueryExecutionTime'], errors='coerce')

            if not np.isnan(val_ours_compile) and val_ours_compile > 0 and \
            not np.isnan(val_compile) and val_compile > 0:
                slowdown = val_compile / val_ours_compile
                compile_vals.append(slowdown)
                print(f"    {query} Compile: {val_compile:.2f} / {val_ours_compile:.2f} = {slowdown:.2f}x")

            if not np.isnan(val_ours_exec) and val_ours_exec > 0 and \
            not np.isnan(val_exec) and val_exec > 0:
                slowdown = val_exec / val_ours_exec
                exec_vals.append(slowdown)
                print(f"    {query} Exec:    {val_exec:.2f} / {val_ours_exec:.2f} = {slowdown:.2f}x")

        except:
            continue

    print(f"  Compile Geomean: {gmean(compile_vals):.2f}x | Exec Geomean: {gmean(exec_vals):.2f}x")
    return compile_vals, exec_vals

# Collect data
all_compile_data = []
all_exec_data = []
xtick_positions = []
xtick_labels = []
group_dividers = []
pos_counter = 1

for label, filename in method_files.items():
    compile_vals, exec_vals = compute_slowdown(label, filename)
    all_compile_data.append(compile_vals)
    all_exec_data.append(exec_vals)
    xtick_positions.append(pos_counter + 0.3)
    xtick_labels.append(label)
    pos_counter += 2
    group_dividers.append(pos_counter - 1)

# Plot
fig, ax = plt.subplots(figsize=(6, 4))
ax.set_yscale('log')
ax.set_ylabel('Relative Slowdown', fontsize=18)
ax.set_ylim(bottom=1e-1, top=10**3.5)
ax.tick_params(axis='y', labelsize=14)

# Layout setup
compile_positions = []
exec_positions = []
spacing = 1.2
start_x = 1
divider_solid = []
x = start_x

for i in range(len(method_files)):
    compile_positions.append(x - 0.2)
    exec_positions.append(x + 0.2)
    x += spacing
    if i < len(method_files) - 1:
        divider_solid.append(x - spacing / 2)

# Draw boxplots
for i in range(len(all_compile_data)):
    ax.boxplot(all_compile_data[i], positions=[compile_positions[i]], widths=0.4,
               patch_artist=True,
               boxprops=dict(facecolor='white', edgecolor='black'),
               medianprops=dict(color='black'),
               whiskerprops=dict(color='black'),
               capprops=dict(color='black'),
               flierprops=dict(markerfacecolor='black', markersize=3))
    ax.boxplot(all_exec_data[i], positions=[exec_positions[i]], widths=0.4,
               patch_artist=True,
               boxprops=dict(facecolor='gray', edgecolor='black'),
               medianprops=dict(color='black'),
               whiskerprops=dict(color='black'),
               capprops=dict(color='black'),
               flierprops=dict(markerfacecolor='black', markersize=3))

# Solid dividers between method groups
for x in divider_solid:
    ax.axvline(x=x, color='black', linestyle='-', linewidth=1)

# Horizontal reference at y=1
ax.axhline(y=1, color='black', linestyle=':', linewidth=1.8)

# X-axis
xtick_positions = [start_x + i * spacing for i in range(len(method_files))]
xtick_labels = list(method_files.keys())
ax.set_xticks(xtick_positions)
ax.set_xticklabels(xtick_labels, fontsize=16)

# Legend
legend_elements = [
    Patch(facecolor='white', edgecolor='black', label='Compile Time'),
    Patch(facecolor='gray', edgecolor='black', label='Execution Time')
]
ax.legend(handles=legend_elements, loc='upper center', bbox_to_anchor=(0.5, 0.995),
          ncol=2, frameon=True, fontsize=14.5, borderaxespad=0.1)

ax.grid(axis='y', linestyle='--', linewidth=0.5)
plt.tight_layout()

# Save figure
output_path = os.path.join(folder_path, 'microbenchmark-summary.png')
plt.savefig(output_path, dpi=800, bbox_inches='tight')
plt.close(fig)

print(f"\n✅ Summary slowdown boxplot saved to: {output_path}")
