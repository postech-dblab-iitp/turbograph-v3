import json
import sys

def generate_bulkloading_script(edge_name_mapping_file, output_script):
    try:
        # Load the JSON file
        with open(edge_name_mapping_file, 'r') as f:
            edge_mapping = json.load(f)
        
        # Open the output script file for writing
        with open(output_script, 'w') as script:
            # Write the header for the bulk loading
            script.write('#!/bin/bash\n\n')
            script.write('data_dir=$1\n')
            script.write('db_dir=$2\n\n')
            
            # Command for bulk loading
            script.write('./execution/bulkload_using_map \\\n')
            script.write('\t--output_dir:${db_dir} \\\n')
            script.write('\t--jsonl:"--file_path:${data_dir}/nodes.json --nodes:NODE" \\\n')

            # Iterate over edge mappings and add relationships to the script
            for edge_label, edge_file in edge_mapping.items():
                script.write(f'\t--relationships:{edge_label} ${{data_dir}}/{edge_file} \\\n')
                script.write(f'\t--relationships_backward:{edge_label} ${{data_dir}}/{edge_file}.backward \\\n')

            # Finish the script
            script.write('\n')
        
        print(f"Bulk loading script generated successfully: {output_script}")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python generate_bulkload_script.py <edge_name_mapping.json> <output_script.sh>")
    else:
        edge_name_mapping_file = sys.argv[1]
        output_script = sys.argv[2]
        generate_bulkloading_script(edge_name_mapping_file, output_script)
