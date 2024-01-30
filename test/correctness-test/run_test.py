import os
import argparse
import logging
import sys
import subprocess
import shlex
import time
from datetime import datetime
from tabulate import tabulate
import csv

# Setup workspace and logging
WORKSPACE = f'logs/{datetime.now().strftime("%Y%m%d-%H%M%S")}'
os.makedirs(WORKSPACE, exist_ok=True)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
output_file_handler = logging.FileHandler(f"{WORKSPACE}/output.log")
stdout_handler = logging.StreamHandler(sys.stdout)
logger.addHandler(output_file_handler)
logger.addHandler(stdout_handler)
temp_file_path = f"{WORKSPACE}/temp.txt"

# Logging header widths
execution_header_widths = [0, 10, 18, 10]

# COMMANDS
ENGINE_COMMAND_BASE = '../../build/tbgpp-client/TurboGraph-S62 --index-join-only --join-order-optimizer:query ' # need to add --query when querying

def parse_query_results(file_path):
	with open(file_path, mode='r', encoding='utf-8') as file:
		csv_reader = csv.DictReader(file, delimiter='|')
		return [row for row in csv_reader]

def load_files_from_directory(directory, parse_csv=False):
	file_contents = {}
	for filename in os.listdir(directory):
		if filename.endswith('.txt'):  # Assuming query and answer files are .txt files
			with open(os.path.join(directory, filename), 'r') as file:
				if parse_csv:
					csv_reader = csv.DictReader(file, delimiter='|')
					file_contents[filename] = [row for row in csv_reader]
				else:
					file_contents[filename] = file.read().strip()
	return file_contents

def print_logger_header():
	logger.debug(f'\n\n====== EXECUTION RESULTS ======')
	headers = ["QUERY", "SUCCESS", "CARDINALITY", "TIME_MS"]
	padded_headers = [ it.rjust(execution_header_widths[idx]) for idx, it in enumerate(headers)]
	header_str = ""
	for h in padded_headers:
		header_str += h
	logger.debug(header_str+'\n')

def simulate_query_execution(db, query_name, query):
	""" Simulate the execution of a query and handle errors """
	try:
		# Run actual query
		query_results = []
		cmd = ENGINE_COMMAND_BASE + f'--workspace:{db} --dump-output {temp_file_path} --query:"{query}"'
		proc = subprocess.Popen(cmd, shell=True, executable="/bin/bash", stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		output, error = proc.communicate()
		output = output.decode('utf-8')
		error = error.decode('utf-8')
		rc = proc.returncode

		if rc == 0 and '[ResultSetSummary]' in output:
			num_card = 0
			avg_exec_time = 0.0
			s = output.split('\n')
			for ss in s:
				if "[ResultSetSummary]" in ss:
					num_card = int(ss.split(' ')[2])
				if "Average Query Execution Time" in ss:
					avg_exec_time = float(ss.split(' ')[-2])
					
			row = [query_name, "SUCCESS", str(num_card), str(avg_exec_time)]
		else:
			row = [query_name, "FAILED", "", ""]
			with open(WORKSPACE+'/'+query_name+'.stderr', 'w') as f:
				f.write(error)
			with open(WORKSPACE+'/'+query_name+'.stdout', 'w') as f:
				f.write(output)

		padded_row = [ it.rjust(execution_header_widths[idx]) for idx, it in enumerate(row)]
		logger.debug(tabulate([padded_row], tablefmt="plain"))
		query_results = parse_query_results(temp_file_path)
		os.remove(temp_file_path)
		return query_results

	except Exception as e:
		logger.error(f"Error executing query: {query}. Error: {str(e)}")
		return None

def compare_results(query_results, answer_results):
    logger.debug(f'\n\n====== TESTING RESULTS ======')
    headers = ["QUERY", "RESULT"]
    data = []

    for query_name, query_result in query_results.items():
        answer_result = answer_results.get(query_name, None)
        result = "SUCCESS" if query_result == answer_result else "FAILED"
        data.append([query_name, result])

    logger.debug(tabulate(data, headers, tablefmt="plain"))
    
def run_background_process_with_args(path, args = ""):
	return subprocess.Popen([path, args], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
if __name__ == "__main__":
	# Setup argument parser
	parser = argparse.ArgumentParser(description='Run S62 Correctness Test')
	parser.add_argument('--database', dest='database', type=str, help='database to run', default='/data/ldbc_test/')
	args = parser.parse_args()
 
	# Run storage and catalog
	storage_process = run_background_process_with_args('../../build/tbgpp-graph-store/store')
	catalog_process = run_background_process_with_args('../../build/tbgpp-graph-store/catalog_test_catalog_server', args.database)
	time.sleep(3)
	print('Storage and catalog started')
 
	# Run test
	print_logger_header()
	queries = load_files_from_directory('./queries', False)
	answers = load_files_from_directory('./answers', True)
	query_results = {name: simulate_query_execution(args.database, name, query) for name, query in queries.items()}
	compare_results(query_results, answers)

	# quit
	storage_process.terminate()
	catalog_process.terminate()