import os
import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import gmean

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
ours_query_time = ours_df.set_index('QueryNumber')['EndtoEndTime']

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

# Function to compute slowdown per method (with logging)
def compute_slowdowns(method_label, method_csv):
    df = pd.read_csv(os.path.join(folder_path, method_csv))
    df = df.set_index('QueryNumber')
    slowdowns = []
    category_map = {cat: [] for cat in categories}

    print(f"\n[{method_label}] Slowdown values compared to 'ours':")
    for query in df.index:
        try:
            val_ours = pd.to_numeric(ours_query_time.get(query), errors='coerce')
            val_other = pd.to_numeric(df.at[query, 'EndtoEndTime'], errors='coerce')
            if not np.isnan(val_ours) and not np.isnan(val_other) and val_ours > 0:
                slowdown = val_other / val_ours
                slowdowns.append(slowdown)
                cat = get_category(query)
                if cat:
                    category_map[cat].append(slowdown)
                print(f"  [{method_label}] {query}: {slowdown:.2f}x")
        except:
            continue

    for cat in categories:
        values = category_map[cat]
        if values:
            print(f"  [{method_label}] Geomean ({cat}): {gmean(values):.2f}x")
        else:
            print(f"  [{method_label}] Geomean ({cat}): N/A")

    if slowdowns:
        print(f"  [{method_label}] Overall Geomean: {gmean(slowdowns):.2f}x")
    else:
        print(f"  [{method_label}] No valid values found.")
    return slowdowns

# Prepare data for boxplot
boxplot_data = []
labels = []
for label, filename in method_files.items():
    data = compute_slowdowns(label, filename)
    boxplot_data.append(data)
    labels.append(label)

# Color mapping
color_mapping = {
    'SA': '#C0C0C0',
    'MA': '#808080',
    'GMMSchema': '#404040'
}

# Create the plot
fig, ax = plt.subplots(figsize=(6, 4))
positions = range(1, len(labels) + 1)

# Plot each box
for i, label in enumerate(labels):
    ax.boxplot(boxplot_data[i], positions=[positions[i]], widths=0.6, patch_artist=True,
               boxprops=dict(facecolor=color_mapping.get(label, '#FFFFFF'),
                             edgecolor='black', linewidth=0.7))

# Styling
ax.set_xticks(positions)
ax.set_xticklabels(labels, rotation=0, fontsize=10)
ax.set_ylabel('Relative Slowdown to Ours', fontsize=12)
ax.set_yscale('log')
ax.grid(axis='y', linestyle='--')
plt.ylim(bottom=0.5, top=100)
plt.tight_layout()

# Save figure
output_path = os.path.join(folder_path, 'relative_slowdown_boxplot.pdf')
plt.savefig(output_path, dpi=800, bbox_inches='tight')
plt.close(fig)

print(f"\nBoxplot saved to: {output_path}")
