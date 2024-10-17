import os
import re
import csv
import argparse

# Function to extract times from a log file
def extract_times_from_log(file_path):
    with open(file_path, 'r') as file:
        content = file.read()

    # Extract compile and execution times using regex
    compile_time = re.search(r'Average Compile Time: ([0-9.]+)', content)
    exec_time = re.search(r'Average Query Execution Time: ([0-9.]+)', content)

    if compile_time and exec_time:
        compile_time = float(compile_time.group(1))
        exec_time = float(exec_time.group(1))
        return compile_time + exec_time
    return None

# Main function to process all log files and output the results in CSV
def process_logs(input_folder, output_csv):
    results = []

    # Iterate over files in the input folder
    for filename in os.listdir(input_folder):
        if filename.endswith(".txt"):
            # Extract query number from filename
            query_match = re.search(r'_Q(\d+)_', filename)
            if query_match:
                query_num = int(query_match.group(1))
                file_path = os.path.join(input_folder, filename)

                # Extract times and calculate total end-to-end time
                total_time = extract_times_from_log(file_path)
                if total_time is not None:
                    results.append((query_num, total_time))

    # Sort results by query number
    results.sort(key=lambda x: x[0])

    # Write results to CSV
    with open(output_csv, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["query_num", "time"])
        writer.writerows(results)

    print(f"Results written to {output_csv}")

if __name__ == "__main__":
    # Set up command-line argument parsing
    parser = argparse.ArgumentParser(description="Process log files and extract end-to-end times.")
    parser.add_argument('input_folder', help="Path to the folder containing log files.")
    parser.add_argument('output_csv', help="Path to the output CSV file.")

    args = parser.parse_args()

    # Process the logs and generate the output CSV
    process_logs(args.input_folder, args.output_csv)
