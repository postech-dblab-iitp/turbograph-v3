import os
import re
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import subprocess
import seaborn as sns

def collect_data(base_dir):
    data = []
    for topology in ["chain", "star", "tree", "cycle", "clique", "petal", "flower", "graph"]:
        topology_dir = os.path.join(base_dir, topology)
        if not os.path.exists(topology_dir):
            continue

        for filename in os.listdir(topology_dir):
            if filename.endswith(".txt"):
                filepath = os.path.join(topology_dir, filename)
                match = re.match(r"yago_Q_(\w+)_(\d+)_(\d+)_(\d+)_(.*).txt", filename)
                if match:
                    T, A, B, C, D = match.groups()
                    try:
                        result = subprocess.check_output(f"grep -a 'Average Query Exec' {filepath} | awk '{{print $5}}'", shell=True)
                        result_str = result.decode('utf-8').strip()
                        if result_str:
                            value = float(result_str)
                            data.append({
                                "topology": T,
                                "size": int(A),
                                "result_size": int(B),
                                "method": D,
                                "exec_time": value
                            })
                    except subprocess.CalledProcessError:
                        continue
    return data

def draw_boxplot(data, x_var, title, xlabel, output_file):
    df = pd.DataFrame(data)
    if x_var not in df.columns:
        print(f"Error: Column '{x_var}' not found in data.")
        return
    
    methods = df['method'].unique()
    x_labels = sorted(df[x_var].unique())
    data_to_plot = []
    
    for label in x_labels:
        label_data = []
        for method in methods:
            method_data = df[(df[x_var] == label) & (df['method'] == method)]['exec_time'].values
            label_data.append(method_data)
        data_to_plot.append(label_data)
    
    fig, ax = plt.subplots(figsize=(15, 10))
    width = 0.2
    x = np.arange(len(x_labels))
    colors = sns.color_palette("husl", len(methods))
    
    for i, method in enumerate(methods):
        positions = x + (i - len(methods) / 2) * width
        bp = ax.boxplot(
            [data[i] for data in data_to_plot],
            positions=positions,
            widths=width,
            patch_artist=True,
            labels=None if i > 0 else x_labels
        )
        for box in bp['boxes']:
            box.set_facecolor(colors[i])
    
    ax.set_yscale('log')
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel('Average Query Execution Time (ms)')
    ax.set_xticks(x)
    ax.set_xticklabels(x_labels, rotation=45)
    ax.legend(methods, title="Methods")
    plt.tight_layout()
    plt.savefig(output_file, format='pdf')
    plt.close()

def main():
    base_dir = "."
    data = collect_data(base_dir)

    if len(data) == 0:
        print("No data collected. Please check the input files.")
        return

    draw_boxplot(
        data,
        x_var="result_size",
        title="Query Execution Time by Result Size",
        xlabel="Query Result Size",
        output_file="query_exec_by_result_size.pdf"
    )

    draw_boxplot(
        data,
        x_var="topology",
        title="Query Execution Time by Topology",
        xlabel="Query Topology",
        output_file="query_exec_by_topology.pdf"
    )

    draw_boxplot(
        data,
        x_var="size",
        title="Query Execution Time by Query Size",
        xlabel="Query Size",
        output_file="query_exec_by_size.pdf"
    )

if __name__ == "__main__":
    main()
