import os
import argparse
from tabulate import tabulate

import time
import logging
import sys
import subprocess
from datetime import datetime
from colorama import init, Fore, Back, Style

init()	# for colorama

BENCHMARKS = ['func', 'ldbc', 'ldbc-simplified']
WORKSPACE = f'logs/{datetime.now().strftime("%Y%m%d-%H%M%S")}'
os.system(f"mkdir -p {WORKSPACE}")

# add logging
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
output_file_handler = logging.FileHandler(f"{WORKSPACE}/output.log")
stdout_handler = logging.StreamHandler(sys.stdout)
logger.addHandler(output_file_handler)
logger.addHandler(stdout_handler)
logger.debug(f'benchmark results are stored in: {WORKSPACE}')

# TODO Fixme for hard coding
DB_LOCATION = "/data/ldbc/sf1_test/"
ENGINE_BIN_LOCATION='../build-debug/tbgpp-client'
#ENGINE_BIN_LOCATION='../build-release/tbgpp-client'
NUM_ITERS = 3
assert NUM_ITERS >= 3
ENGINE_COMMAND_BASE = ENGINE_BIN_LOCATION + f'/TurboGraph-S62 --workspace:{DB_LOCATION} --index-join-only --num-iterations:{NUM_ITERS}' # need to add --query when querying
PARAM_SIZE = 5

def prepare_ldbc_queries(benchmark, SF):
	query_dir = f"queries/{benchmark}/"
	query_files = [query_dir + f for f in os.listdir(query_dir) if os.path.isfile(os.path.join(query_dir, f)) and f.endswith('.cypher')]

	sub_dir_base = "substitution_parameters/ldbc_hadoop_substitution_parameters/"
	complex_substitution_dir = sub_dir_base + f'interactive-complex/sf{SF}/substitution_parameters/'
	short_substitution_dir = sub_dir_base + f'interactive-short/sf{SF}/'

	prepared_queries = {}
	for query_file in query_files:
		# pass updates
		if 'update' in query_file:
			continue
		
		with open(query_file, 'r') as q:
			lines = q.readlines()
		# delete comments
		for idx, line in enumerate(lines):
			if line.startswith('//'):
				del lines[idx]

		# delete parameters
		st, en = 0,0
		for idx, line in enumerate(lines):
			if '/*' in line:
				st = idx
			if '*/' in line:
				en = idx
		if st != 0 or en != 0: # comment detected
			del lines[st:en+1]
		
		query_str = ''.join(lines)
		filename = os.path.split(query_file)[-1]
		query_name = filename.replace('.cypher', '')
		# substitute
		if 'short' in query_file:
			qid = int(filename.replace('interactive-short-', '').replace('.cypher', ''))
			subfile = short_substitution_dir + '/short_'+str(qid)+'_param.txt'
			with open(subfile, 'r', encoding="utf-8") as f:
				lines = f.readlines()
			prepared_queries[query_name] = []
			keys = lines[0].strip('\n').split('|')
			for line in lines[1:PARAM_SIZE+1]:
				prepared_query_str = query_str
				# replace personId or messageId
				values = line.strip('\n').split('|')
				for idx, key in enumerate(keys):
					prepared_query_str = prepared_query_str.replace('$'+key, values[idx])
				prepared_queries[query_name].append(prepared_query_str)

		elif 'complex' in query_file:
			qid = int(filename.replace('interactive-complex-', '').replace('.cypher', ''))
			subfile = complex_substitution_dir + '/interactive_'+str(qid)+'_param.txt'
			with open(subfile, 'r', encoding="utf-8") as f:
				lines = f.readlines()
			prepared_queries[query_name] = []
			keys = lines[0].strip('\n').split('|')
			for line in lines[1:PARAM_SIZE+1]:
				prepared_query_str = query_str
				values = line.strip('\n').split('|')
				for idx, key in enumerate(keys):
					if 'Name' in key:
						# string value
						prepared_query_str = prepared_query_str.replace('$'+key, f'"{values[idx]}"')
					else:
						# others
						prepared_query_str = prepared_query_str.replace('$'+key, values[idx])
				# haneld endDate
				startDateIdx = 0
				durationDaysIdx = 0
				for idx, key in enumerate(keys):
					if key == 'startDate':
						startDateIdx = idx
					if key == 'durationDays':
						durationDaysIdx = idx
				if startDateIdx != durationDaysIdx: # found
					endDate = str( int(values[startDateIdx]) + int(values[durationDaysIdx] ) )
					prepared_query_str = prepared_query_str.replace('$endDate', endDate)
				
				prepared_queries[query_name].append(prepared_query_str)
	return prepared_queries

def prepare_normal_queries(benchmark):
	query_dir = f"queries/{benchmark}/"
	query_files = [query_dir + f for f in os.listdir(query_dir) if os.path.isfile(os.path.join(query_dir, f))]

	prepared_queries = {}
	for query_file in query_files:
		with open(query_file, 'r') as q:
			lines = q.readlines()
		for idx, line in enumerate(lines):
			if line.startswith('//'):
				del lines[idx]
			if line == '\n':
				del lines[idx]

		query_strs_whole = ''.join(lines).replace('\n', '')
		filename = os.path.split(query_file)[-1]
		query_strs = query_strs_whole.split(';')
		for idx, st in enumerate(query_strs):
			if st == '' or st == '\n':
				del query_strs[idx]
		for idx, q in enumerate(query_strs):
			prepared_queries[filename+'#'+str(idx)] = [q,]	# only one parameter!
		
	return prepared_queries

def run_benchmark(benchmark, SF):
	if benchmark == 'ldbc' or benchmark == 'ldbc_simplified':
		queries = prepare_ldbc_queries(benchmark, SF)
	else:
		queries = prepare_normal_queries(benchmark)
	
	# queries = dict(query_name, list of queries with different params)
	BM_WORKSPACE = WORKSPACE+'/'+benchmark+'/'
	os.system(f"mkdir -p {BM_WORKSPACE}")

	logger.debug(f'\n\n====== BENCHMARK : {benchmark} ======')
	headers = ["QUERY", "PARAM_IDX", "SUCCESS", "RES_CARD", "TIME_MS"]
	rows = []
	for query_name in queries.keys():
		querysets  = queries[query_name]
		for query_idx, query in enumerate(querysets):
			cmd = ENGINE_COMMAND_BASE+f' --query:"{query}"'
			proc = subprocess.Popen(cmd, shell=True, executable="/bin/bash", stdout=subprocess.PIPE, stderr=subprocess.PIPE)
			output, error = proc.communicate()
			output = output.decode('utf-8')
			error = error.decode('utf-8')
			rc = proc.returncode

			# check
			if rc == 0 and '[ResultSetSummary]' in output:
				num_card = 0
				avg_exec_time = 0.0
				s = output.split('\n')
				for ss in s:
					if "[ResultSetSummary]" in ss:
						num_card = int(ss.split(' ')[2])
					if "Average Query Execution Time" in ss:
						avg_exec_time = float(ss.split(' ')[-2])
						
				rows.append([query_name, str(query_idx), Fore.GREEN + "SUCCESS" + Fore.RESET, str(num_card), str(avg_exec_time)])
			else:
				rows.append([query_name, str(query_idx), Fore.RED + "FAILED" + Fore.RESET, "", ""])
				# flush error cases to file.
				filename_base = query_name + "_" + str(query_idx)
				with open(BM_WORKSPACE+filename_base+'.stderr', 'w') as f:
					f.write(error)
				with open(BM_WORKSPACE+filename_base+'.stdout', 'w') as f:
					f.write(output)

	# print bench result
	logger.debug(tabulate(rows, headers))

if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='Run S62 benchmark')
	parser.add_argument('benchmarks', metavar='BM', type=str, nargs='+',
                    help='benchmarks to run')
	parser.add_argument('--SF', dest='SF', type=int, help='scale factor (default=1), used when selecting substitution params for LDBC', default=1)

	args = parser.parse_args()
	
	#check valid
	selected_benchmarks = []
	for bm in args.benchmarks:
		if bm == "all":
			selected_benchmarks = BENCHMARKS
			break
		assert bm in BENCHMARKS, f"Provided benchmark '{bm}' is not in supported benchmarks"
		selected_benchmarks.append(bm)
	SF = args.SF

	for bm in selected_benchmarks:
		run_benchmark(bm, SF)