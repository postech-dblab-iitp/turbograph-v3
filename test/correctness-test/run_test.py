import os
import argparse
import logging
import sys
import subprocess
import shlex
from datetime import datetime
from colorama import init, Fore, Back, Style
from tabulate import tabulate

init()  # for colorama

# Setup workspace and logging
WORKSPACE = f'logs/{datetime.now().strftime("%Y%m%d-%H%M%S")}'
os.makedirs(WORKSPACE, exist_ok=True)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
output_file_handler = logging.FileHandler(f"{WORKSPACE}/output.log")
stdout_handler = logging.StreamHandler(sys.stdout)
logger.addHandler(output_file_handler)
logger.addHandler(stdout_handler)

# COMMANDS
ENGINE_COMMAND_BASE = '../../tbgpp-client/TurboGraph-S62 --index-join-only --join-order-optimizer:query ' # need to add --query when querying

def load_files_from_directory(directory):
    """ Load files from a given directory """
    file_contents = {}
    for filename in os.listdir(directory):
        if filename.endswith('.txt'):  # Assuming query and answer files are .txt files
            with open(os.path.join(directory, filename), 'r') as file:
                file_contents[filename] = file.read().strip()
    return file_contents

def simulate_query_execution(db, query_name, query):
	""" Simulate the execution of a query and handle errors """
	try:
		# Setup logging
		logger.debug(f'\n\n====== CORRECTNESS TESTING ======')
		headers = ["QUERY", "SUCCESS", "RES_CARD", "TIME_MS"]
		header_widths = [30, 10, 15, 10]
		padded_headers = [ it.rjust(header_widths[idx]) for idx, it in enumerate(headers)]
		header_str = ""
		for h in padded_headers:
			header_str += h
		logger.debug(header_str+'\n')

		# Run actual query
		rows = []
		cmd = ENGINE_COMMAND_BASE + f'--workspace:{db} --query:"{query}"'
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
					
			rows.append([query_name, Fore.GREEN + "SUCCESS" + Fore.RESET, str(num_card), str(avg_exec_time)])
		else:
			row = [query_name, Fore.RED + "FAILED" + Fore.RESET, "", ""]
			rows.append(row)
			# flush error cases to file.
			with open(WORKSPACE+'/'+query_name+'.stderr', 'w') as f:
				f.write(error)
			with open(WORKSPACE+'/'+query_name+'.stdout', 'w') as f:
				f.write(output)
     
		padded_rows = []
		for row in rows:
			padded_rows.append([ it.rjust(header_widths[idx]) for idx, it in enumerate(row)])
		logger.debug(tabulate(padded_rows, tablefmt="plain"))

	except Exception as e:
		logger.error(f"Error executing query: {query}. Error: {str(e)}")
		return "INVALID_RESULT"

def compare_results(query_results, answer_results):
    """ Compare query results with answer results """
    headers = ["QUERY", "STATUS", "RESULT"]
    data = []

    for query_name, query_result in query_results.items():
        answer_result = answer_results.get(query_name, None)
        status = Fore.GREEN + "SUCCESS" + Fore.RESET if query_result == answer_result else Fore.RED + "FAILED" + Fore.RESET
        data.append([query_name, status, query_result])

    logger.debug(tabulate(data, headers, tablefmt="plain"))
    
def run_background_process(path):
    return subprocess.Popen([path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
if __name__ == "__main__":
	# Setup argument parser
	parser = argparse.ArgumentParser(description='Run S62 Correctness Test')
	parser.add_argument('--database', dest='database', type=str, help='database to run', default='/data/ldbc/sf1_test/')
	args = parser.parse_args()
 
	# Run storage and catalog
	storage_process = run_background_process('../../build/tbgpp-store/store')
	catalog_process = run_background_process('../../build/tbgpp-store/catalog_test_catalog_server ' + args.database)
 
	# Run test
	queries = load_files_from_directory('./queries')
	answers = load_files_from_directory('./answers')
	query_results = {name: simulate_query_execution(args.database, name, query) for name, query in queries.items()}
	compare_results(query_results, answers)
  
	# wait
	storage_process.wait()
	catalog_process.wait()