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
    'Scan': range(2, 31),
    'Sel': range(31, 71)
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
def compute_slowdown_per_category(method_label, method_csv):
    df = pd.read_csv(os.path.join(folder_path, method_csv)).set_index('QueryNumber')
    ours_compile = ours_df.set_index('QueryNumber')['CompileTime']
    ours_exec = ours_df.set_index('QueryNumber')['QueryExecutionTime']

    result = {cat: {'compile': [], 'exec': []} for cat in categories}

    # Also collect all and scan+sel
    all_compile = []
    all_exec = []
    scan_sel_compile = []
    scan_sel_exec = []

    print(f"\n[{method_label}] Slowdowns per category:")
    for query in df.index:
        try:
            qnum = int(query[1:])
            val_ours_compile = pd.to_numeric(ours_compile.get(query), errors='coerce')
            val_ours_exec = pd.to_numeric(ours_exec.get(query), errors='coerce')
            val_compile = pd.to_numeric(df.at[query, 'CompileTime'], errors='coerce')
            val_exec = pd.to_numeric(df.at[query, 'QueryExecutionTime'], errors='coerce')

            for cat, rng in categories.items():
                if qnum in rng:
                    if not np.isnan(val_ours_compile) and val_ours_compile > 0 and \
                       not np.isnan(val_compile) and val_compile > 0:
                        slowdown = val_compile / val_ours_compile
                        result[cat]['compile'].append(slowdown)
                        all_compile.append(slowdown)
                        if cat in ['Scan', 'Sel']:
                            scan_sel_compile.append(slowdown)

                    if not np.isnan(val_ours_exec) and val_ours_exec > 0 and \
                       not np.isnan(val_exec) and val_exec > 0:
                        slowdown = val_exec / val_ours_exec
                        result[cat]['exec'].append(slowdown)
                        all_exec.append(slowdown)
                        if cat in ['Scan', 'Sel']:
                            scan_sel_exec.append(slowdown)
        except:
            continue

    for cat in categories:
        c_vals = result[cat]['compile']
        e_vals = result[cat]['exec']
        print(f"  {cat} → Compile Geomean: {gmean(c_vals):.2f}x | Exec Geomean: {gmean(e_vals):.2f}x")

    # Extra: Scan+Sel and All
    if scan_sel_compile:
        print(f"  Scan+Sel → Compile Geomean: {gmean(scan_sel_compile):.2f}x | Exec Geomean: {gmean(scan_sel_exec):.2f}x")
    if all_compile:
        print(f"  All → Compile Geomean: {gmean(all_compile):.2f}x | Exec Geomean: {gmean(all_exec):.2f}x")

    return result

# Collect data
all_compile_data = []
all_exec_data = []
xtick_positions = []
xtick_labels = []
group_dividers = []
subgroup_dividers = []
pos_counter = 1

for label, filename in method_files.items():
    per_cat_data = compute_slowdown_per_category(label, filename)
    for cat in ['Scan', 'Sel']:
        # Append compile and exec data for this category
        all_compile_data.append(per_cat_data[cat]['compile'])
        all_exec_data.append(per_cat_data[cat]['exec'])
        xtick_positions.append(pos_counter + 0.3)
        xtick_labels.append(f"{label}\n{cat}")
        # Track positions
        pos_counter += 1
        if cat != 'Sel':
            subgroup_dividers.append(pos_counter - 1)
    group_dividers.append(pos_counter - 1)  # solid divider after each method

# Plot
fig, ax = plt.subplots(figsize=(6, 3))
ax.set_yscale('log')
ax.set_ylabel('Relative Slowdown', fontsize=19)
ax.set_ylim(bottom=10**(-1.37), top=10**3.9)
ax.tick_params(axis='y', labelsize=14)

# Layout setup
total_groups = len(method_files)
categories_per_group = 3
boxplots_per_category = 2
spacing = 1  # spacing between Scan, Sel
start_x = 1

compile_positions = []
exec_positions = []
xtick_positions = []
xtick_labels = []
divider_solid = []
divider_dotted = []
group_starts = []

x = start_x
for method in method_files:
    group_starts.append(x)
    for cat in ['Scan', 'Sel']:
        exec_positions.append(x)
        x += spacing
        if cat != 'Sel':
            divider_dotted.append(x - spacing / 2)
    if method != list(method_files)[-1]:
        divider_solid.append(x - spacing / 2)

# Draw boxplots
for i in range(len(all_compile_data)):

    # Draw Execution Time boxplot
    ax.boxplot(all_exec_data[i], positions=[exec_positions[i]], widths=0.8,
               patch_artist=True,
               boxprops=dict(facecolor='gray', edgecolor='black'),
               medianprops=dict(color='black'),
               whiskerprops=dict(color='black'),
               capprops=dict(color='black'),
               flierprops=dict(markerfacecolor='black', markersize=4))
    
    # Geomean marker for Exec
    if len(all_exec_data[i]) > 0:
        exec_geo = gmean(all_exec_data[i])
        ax.plot(exec_positions[i], exec_geo, marker='x', color='black', markersize=8, linestyle='None')

# Solid dividers between method groups
for x in divider_solid:
    ax.axvline(x=x, color='black', linestyle='-', linewidth=1)

# Dotted dividers between categories
for x in divider_dotted:
    ax.axvline(x=x, color='black', linestyle=':', linewidth=1)

# Horizontal reference at y=1
ax.axhline(y=1, color='black', linestyle=':', linewidth=1.8)

# X-axis: method names only
xtick_labels = list(method_files.keys())
xtick_positions = [start + 0.5 for start in group_starts]  # middle of each method block
ax.set_xticks(xtick_positions)
ax.set_xticklabels(xtick_labels, fontsize=19)

# In-plot labels for Scan/Sel
for group_idx, method in enumerate(method_files):
    for cat_idx, cat in enumerate(['Scan', 'Scan\n+Sel']):
        xpos = group_starts[group_idx] + cat_idx * spacing
        if cat == 'Scan\n+Sel':
            label_ypos = 0.17  # lower for Scan+Sel
        else:
            label_ypos = 0.12
        ax.text(xpos, label_ypos, cat,
                transform=ax.get_xaxis_transform(),
                ha='center', va='top',
                fontsize=13, style='italic')

# Legend
legend_elements = [
    Patch(facecolor='gray', edgecolor='black', label='Execution Time')
]
ax.legend(handles=legend_elements, loc='upper left',
          ncol=1, frameon=True, fontsize=13, borderaxespad=0.3, columnspacing=0)

ax.grid(axis='y', linestyle='--', linewidth=0.5)
plt.tight_layout()

# Save figure
output_path = os.path.join(folder_path, 'cgc-microbenchmark.pdf')
plt.savefig(output_path, dpi=800, bbox_inches='tight')
plt.close(fig)

print(f"\nFine-grained slowdown boxplot saved to: {output_path}")