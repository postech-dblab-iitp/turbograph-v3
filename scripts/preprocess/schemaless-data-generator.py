import sys
import csv
import yaml
import os
import random
import json
from concurrent.futures import ProcessPoolExecutor
from itertools import combinations
import time
import math

#####################
###    DEFINES    ###
#####################

SCALE_FACTOR = 1
SCALE_FOLDER = 'sf' + str(SCALE_FACTOR)

# DBs
DUCKDB_NAME = 'duckdb'
KUZU_NAME = 'kuzu'
NEO4J_NAME = 'neo4j'
S62_NAME = 's62'
POSTGRESQL_NAME = 'postgresql'

# Folders
DUCKDB_FOLDER = DUCKDB_NAME
KUZU_FOLDER = KUZU_NAME
NEO4J_FOLDER = NEO4J_NAME
S62_FOLDER = S62_NAME
POSTGRESQL_FOLDER = POSTGRESQL_NAME

# postfixs
DUCKDB_CSV_POSTFIX = '_0_0.csv'
KUZU_CSV_POSTFIX = '_0_0.csv'
NEO4J_CSV_POSTFIX = '_0_0.csv'
S62_CSV_POSTFIX = '.csv'
S62_JSON_POSTFIX = '.json'
POSTGRESQL_CSV_POSTFIX = '_0_0.csv'

# others
COLUMN_TYPE = 'LONG'
NEW_COL_PREFIX = 'new_col_'
CONFIG_NAME = ''
DATASET_BASE_PATH = ''
OUTPUT_BASE_PATH = ''

#####################
### UTIL FUNCTIONS ###
#####################

def printAndExit(print_string):
    print(print_string)
    sys.exit()
    
def getNewColumnName(db_name, column_index):
    if db_name == DUCKDB_NAME:
        return NEW_COL_PREFIX + str(column_index)
    elif db_name == KUZU_NAME:
        return NEW_COL_PREFIX + str(column_index) + ":" + COLUMN_TYPE 
    elif db_name == NEO4J_NAME:
        return NEW_COL_PREFIX + str(column_index) + ":" + COLUMN_TYPE
    elif db_name == S62_NAME:
        return NEW_COL_PREFIX + str(column_index)
    elif db_name == POSTGRESQL_NAME:
        return NEW_COL_PREFIX + str(column_index)

def getInputDBPostfix(db_name):
    if db_name == DUCKDB_NAME:
        return DUCKDB_CSV_POSTFIX
    elif db_name == KUZU_NAME:
        return KUZU_CSV_POSTFIX
    elif db_name == NEO4J_NAME:
        return NEO4J_CSV_POSTFIX
    elif db_name == S62_NAME:
        return S62_CSV_POSTFIX
    elif db_name == POSTGRESQL_NAME:
        return POSTGRESQL_CSV_POSTFIX

def getOutputDBPostfix(db_name):
    if db_name == DUCKDB_NAME:
        return DUCKDB_CSV_POSTFIX
    elif db_name == KUZU_NAME:
        return KUZU_CSV_POSTFIX
    elif db_name == NEO4J_NAME:
        return NEO4J_CSV_POSTFIX
    elif db_name == S62_NAME:
        return S62_JSON_POSTFIX
    elif db_name == POSTGRESQL_NAME:
        return POSTGRESQL_CSV_POSTFIX

def getDBFolder(db_name):
    if db_name == DUCKDB_NAME:
        return DUCKDB_FOLDER
    elif db_name == KUZU_NAME:
        return KUZU_FOLDER
    elif db_name == NEO4J_NAME:
        return NEO4J_FOLDER
    elif db_name == S62_NAME:
        return S62_FOLDER
    elif db_name == POSTGRESQL_NAME:
        return POSTGRESQL_FOLDER
    
def getNodeFilePrefix(db_name, node_name):
    if db_name == S62_NAME:
        return node_name[0].upper() + node_name[1:]
    else:
        return node_name

def isOutputFormatCSV(db_name):
    if db_name == DUCKDB_NAME:
        return True
    elif db_name == KUZU_NAME:
        return True
    elif db_name == NEO4J_NAME:
        return True
    elif db_name == S62_NAME:
        return False
    elif db_name == POSTGRESQL_NAME:
        return True
    else:
        return False

def getColumnNameWithoutType(column_name):
    return column_name.split(':')[0]

def castValueToType(column_str, value):
    type_string = column_str.split(':')[1].lower()
    if value == '':
        return ''
    elif 'id' in type_string:
        return int(value)
    elif 'int' in type_string:
        return int(value)
    elif 'float' in type_string:
        return float(value)
    elif 'long' in type_string:
        return int(value)
    else:
        return value

#####################
### VAL FUNCTIONS ###
#####################

# Validate the given configuration file
def validateConfigurationFile(configuration_yaml):
    # Check name is given
    partitions = configuration_yaml['name']
    if partitions is None:
        printAndExit('No name given')
        
    # Check dataset_base_path is given
    dataset_base_path = configuration_yaml['dataset_base_path']
    if dataset_base_path is None:
        printAndExit('No dataset_base_path given')
    
    # Check output_base_path is given
    output_base_path = configuration_yaml['output_base_path']
    if output_base_path is None:
        printAndExit('No output_base_path given')
    
    # Check nodes if given
    nodes = configuration_yaml['nodes']
    if nodes is None:
        printAndExit('No nodes given')
    
    # For each node, check configuration
    for node in nodes:
        # Check type is given and is valid (i.e., dynamic or static)
        node_type = nodes[node]['type']
        if node_type is None or (node_type != 'dynamic' and node_type != 'static'):
            printAndExit('Invalid node type given')
        
        # Check num_schemas is given and >= 1
        num_schemas = nodes[node]['num_schemas']
        if num_schemas is None or num_schemas < 0:
            printAndExit('Invalid num_schemas given')
            
        # Check if num_schemas is 1, then no need for mode
        if (num_schemas == 0):
            return
        
        # Check mode is given and add_column or remove_column
        mode = nodes[node]['mode']
        if mode is None or (mode != 'add_column_seq' and mode != 'add_column_inclusive' and mode != 'remove_column' and mode != 'add_column_comb'):
            printAndExit('Invalid mode given')
        
        # Check if having rm_cols if mode is remove_column
        if mode == 'remove_column':
            rm_cols = nodes[node]['rm_cols']
            if rm_cols is None or len(rm_cols) == 0:
                printAndExit('No rm_cols given for remove_column mode')
        
        # Check shuffle is given and is boolean
        shuffle = nodes[node]['shuffle']
        if shuffle is None or (shuffle != True and shuffle != False):
            printAndExit('Invalid shuffle given')

def validateArguments():
    if len(sys.argv) != 2:
        print('Invalid arguments')
        sys.exit()
        
#####################
### GEN FUNCTIONS ###
#####################

def getCombinations(cols, n):
    return list(combinations(cols, n))

def assignColumnsToSchemasComb(num_schemas, rm_cols):
    schema_rm_columns = [[] for _ in range(num_schemas)]  # Initialize with empty lists for no removal
    if num_schemas - 1 <= len(rm_cols):
        # Assign one unique column to each schema, except the first
        for i in range(1, num_schemas):
            schema_rm_columns[i].append(rm_cols[i - 1])
    else:
        # Generate combinations of increasing size until all schemas are assigned
        assigned_schemas = 1  # Start from 1 to leave the first schema with no removals
        for r in range(1, len(rm_cols) + 1):
            if assigned_schemas >= num_schemas:
                break
            for combo in getCombinations(rm_cols, r):
                if assigned_schemas >= num_schemas:
                    break
                schema_rm_columns[assigned_schemas].extend(combo)
                assigned_schemas += 1
    return schema_rm_columns

def getSchemaDistributionArray(num_schemas, null_percentage):
    # Create distribution array
    num_slots = 10 * num_schemas
    num_non_null = int(num_slots * ((100 - null_percentage) / 100.0))
    distribution_array = [False] * num_slots  # Start with all False

    # Distribute True values evenly across the array
    if num_non_null > 0:
        interval = num_slots / num_non_null
        for i in range(num_non_null):
            index = int(round(i * interval))
            if index < num_slots:
                distribution_array[index] = True # True means non null
    return num_slots, distribution_array

def assignColumnsToSchemasBinary(num_schemas, rm_cols):
    num_columns_to_remove = int(math.log(num_schemas, 2))
    
    # Adjust num_schemas if there are fewer columns than required
    if len(rm_cols) < num_columns_to_remove:
        num_columns_to_remove = len(rm_cols)
        num_schemas = 2 ** num_columns_to_remove

    schema_rm_columns = [[] for _ in range(num_schemas)]

    # Assign columns to schemas using binary representation
    for i in range(num_schemas):
        binary_representation = format(i, f'0{num_columns_to_remove}b')
        for j, char in enumerate(binary_representation):
            if char == '1':
                schema_rm_columns[i].append(rm_cols[j])

    return schema_rm_columns

def generateRemoveColumnNodeJSONFile(node_name, num_schemas, rm_cols, shuffle, null_percentage, input_file_path, output_file_path):
    # Assign columns to remove for each schema based on the logic described previously
    schema_rm_columns = assignColumnsToSchemasBinary(num_schemas, rm_cols)
    num_slots, distribution_array = getSchemaDistributionArray(num_schemas, null_percentage)

    # Read the CSV and prepare the JSON output
    with open(input_file_path, 'r', newline='', encoding='utf-8') as infile, open(output_file_path, 'w', newline='', encoding='utf-8') as outfile:
        reader = csv.reader(infile, delimiter='|')
        headers = next(reader)  # Extract headers to use as property names
        
        # Iterate over each row in the CSV file
        current_non_null_tuples = 0
        for row_index, row in enumerate(reader):
            slot_idx = row_index % num_slots
            if distribution_array[slot_idx]:
                schema_index = 0
            else:
                current_non_null_tuples += 1
                schema_index = 1 + (current_non_null_tuples % (num_schemas - 1))
            cols_to_remove = schema_rm_columns[schema_index]

            # Create the base JSON object
            json_object = {
                "labels": [node_name.lower()],
                "properties": {}
            }

            # Fill in the properties from the row, excluding the ones to remove
            for header, value in zip(headers, row):
                if getColumnNameWithoutType(header) not in cols_to_remove:
                    json_object["properties"][getColumnNameWithoutType(header)] = castValueToType(header, value)
            
            # Write the JSON object to the file
            json.dump(json_object, outfile)
            outfile.write('\n')  # Add a newline to separate JSON objects

def generateRemoveColumnNodeCSVFile(num_schemas, rm_cols, shuffle, null_percentage, input_file_path, output_file_path):
    schema_rm_columns = assignColumnsToSchemasBinary(num_schemas, rm_cols)
    num_slots, distribution_array = getSchemaDistributionArray(num_schemas, null_percentage)

    with open(input_file_path, mode='r', newline='', encoding='utf-8') as infile, \
         open(output_file_path, mode='w', newline='', encoding='utf-8') as outfile:

        reader = csv.reader(infile, delimiter='|')
        writer = csv.writer(outfile, delimiter='|')

        headers = next(reader)
        writer.writerow(headers)
        headers_without_type = [getColumnNameWithoutType(header) for header in headers]

        rm_col_indices_per_schema = []
        for schema_rm_cols in schema_rm_columns:
            rm_col_indices = []
            for rm_col in schema_rm_cols:
                try:
                    col_index = headers_without_type.index(rm_col)
                    rm_col_indices.append(col_index)
                except ValueError:
                    print(f"Column {rm_col} not found in header")
                    return
            rm_col_indices_per_schema.append(rm_col_indices)
            
        current_non_null_tuples = 0
        for row_index, row in enumerate(reader):
            slot_idx = row_index % num_slots
            if distribution_array[slot_idx]:
                schema_index = 0
            else:
                current_non_null_tuples += 1
                schema_index = 1 + (current_non_null_tuples % (num_schemas - 1))
            cols_to_remove = rm_col_indices_per_schema[schema_index]
            new_row = [col if i not in cols_to_remove else None for i, col in enumerate(row)]
            writer.writerow(new_row)

def assignColumnsToSchemas(num_schemas, rm_cols, null_percentage):
    total_attributes = num_schemas * (len(rm_cols) + 1)  # Add 1 for the primary key
    total_nulls = math.ceil(total_attributes * (null_percentage / 100))
    nulls_per_schema = total_nulls // (num_schemas - 1) if num_schemas > 1 else 0

    # Adjust the last schema to accommodate any remainder nulls not evenly distributed
    remainder_nulls = total_nulls % (num_schemas - 1) if num_schemas > 1 else 0

    schema_rm_columns = [[] for _ in range(num_schemas)]
    column_index = 0

    # Start with the second schema (index 1) because the first schema (index 0) has no nulls
    for schema_index in range(1, num_schemas):
        null_count = nulls_per_schema
        if schema_index == num_schemas - 1:
            null_count += remainder_nulls  # Add remainder to the last schema
        
        schema_rm_columns[schema_index] = rm_cols[column_index:column_index + null_count]
        column_index = (column_index + null_count) % len(rm_cols)

    return schema_rm_columns

def generateRemoveColumnNodeJSONFile2(node_name, num_schemas, rm_cols, shuffle, null_percentage, input_file_path, output_file_path):
    schema_rm_columns = assignColumnsToSchemasComb(num_schemas, rm_cols)

    # Read the CSV and prepare the JSON output
    with open(input_file_path, 'r', newline='', encoding='utf-8') as infile, open(output_file_path, 'w', newline='', encoding='utf-8') as outfile:
        reader = csv.reader(infile, delimiter='|')
        headers = next(reader)  # Extract headers to use as property names
        
        # Iterate over each row in the CSV file\
        for row_index, row in enumerate(reader):
            schema_index = row_index % num_schemas
            cols_to_remove = schema_rm_columns[schema_index]

            # Create the base JSON object
            json_object = {
                "labels": [node_name.lower()],
                "properties": {}
            }

            # Fill in the properties from the row, excluding the ones to remove
            for header, value in zip(headers, row):
                if getColumnNameWithoutType(header) not in cols_to_remove:
                    json_object["properties"][getColumnNameWithoutType(header)] = castValueToType(header, value)
            
            # Write the JSON object to the file
            json.dump(json_object, outfile)
            outfile.write('\n')  # Add a newline to separate JSON objects

def generateRemoveColumnNodeCSVFile2(num_schemas, rm_cols, null_percentage, input_file_path, output_file_path):
    schema_rm_columns = assignColumnsToSchemasComb(num_schemas, rm_cols)
    
    with open(input_file_path, mode='r', newline='', encoding='utf-8') as infile, \
         open(output_file_path, mode='w', newline='', encoding='utf-8') as outfile:

        reader = csv.reader(infile, delimiter='|')
        writer = csv.writer(outfile, delimiter='|')

        headers = next(reader)
        writer.writerow(headers)
        headers_without_type = [getColumnNameWithoutType(header) for header in headers]

        rm_col_indices_per_schema = []
        for schema_rm_cols in schema_rm_columns:
            rm_col_indices = []
            for rm_col in schema_rm_cols:
                try:
                    col_index = headers_without_type.index(rm_col)
                    rm_col_indices.append(col_index)
                except ValueError:
                    print(f"Column {rm_col} not found in header")
                    return
            rm_col_indices_per_schema.append(rm_col_indices)

        for row_index, row in enumerate(reader):
            schema_index = row_index % num_schemas
            cols_to_remove = rm_col_indices_per_schema[schema_index]
            new_row = [col if i not in cols_to_remove else None for i, col in enumerate(row)]
            writer.writerow(new_row)

def generateRemoveColumnNodeFile(db_name, node_name, num_schemas, rm_cols, shuffle, null_percentage, input_file_path, output_file_path):
    is_csv = isOutputFormatCSV(db_name)
    
    if is_csv:
        generateRemoveColumnNodeCSVFile2(num_schemas, rm_cols, null_percentage, input_file_path, output_file_path)
    else:
        generateRemoveColumnNodeJSONFile2(node_name, num_schemas, rm_cols, shuffle, null_percentage, input_file_path, output_file_path)
    return

def generateAddColumnSequentialNodeJSONFile(db_name, node_name, num_schemas, shuffle, input_file_path, output_file_path):
    # Step 1: Read the CSV and prepare the JSON output
    with open(input_file_path, 'r', newline='', encoding='utf-8') as infile, open(output_file_path, 'w', newline='', encoding='utf-8') as outfile:
        reader = csv.reader(infile, delimiter='|')
        headers = next(reader)  # Extract headers to use as property names
        
        # Iterate over each row in the CSV file
        for row_index, row in enumerate(reader):
            # Create the base JSON object
            json_object = {
                "labels": [node_name.lower()],
                "properties": {}
            }

            # Fill in the properties from the row
            for header, value in zip(headers, row):
                json_object["properties"][getColumnNameWithoutType(header)] = castValueToType(header, value)  # Assuming direct mapping without type casting
            
            # Add additional properties based on schema logic
            partition_index = (row_index % num_schemas if shuffle else row_index // (len(reader) // num_schemas))
            deterministic_random = abs(hash((row_index, partition_index))) % 101
            new_property_name = getColumnNameWithoutType(getNewColumnName(db_name, partition_index))
            json_object["properties"][new_property_name] = deterministic_random  # Adding new property
            
            # Write the JSON object to the file
            json.dump(json_object, outfile)
            outfile.write('\n')  # Add a newline to separate JSON objects

def generateAddColumnSequentialNodeCSVFile(db_name, num_schemas, shuffle, insert_location, input_file_path, output_file_path):
    # Step 1: Determine the UNION schema
    with open(input_file_path, 'r', newline='', encoding='utf-8') as file:
        reader = csv.reader(file, delimiter='|')
        headers = next(reader)
        new_column_names = [getNewColumnName(db_name, i) for i in range(num_schemas)]
        
        # Determine the position to insert new columns
        if insert_location == 'head':
            union_schema = new_column_names + headers
        else:  # Default to 'tail' if 'head' is not specified
            union_schema = headers + new_column_names
        
    # Step 2: Process tuples to conform to the UNION schema
    with open(input_file_path, 'r', newline='', encoding='utf-8') as infile, open(output_file_path, 'w', newline='', encoding='utf-8') as outfile:
        reader = csv.reader(infile, delimiter='|')
        writer = csv.writer(outfile, delimiter='|')
        
        next(reader)  # Skip header row
        writer.writerow(union_schema)  # Write the UNION schema

        for row_index, row in enumerate(reader):
            # Initialize new_row based on the insert_location
            partition_index = (row_index % num_schemas if shuffle else row_index // (len(reader) // num_schemas))
            deterministic_random = abs(hash((row_index, partition_index))) % 101
            if insert_location == 'head':
                new_row = [None] * num_schemas + row
                new_row[partition_index] = deterministic_random
            else:  # Default to 'tail' if 'head' is not specified
                new_row = row + [None] * num_schemas
                new_row[len(headers) + partition_index] = deterministic_random
            
            writer.writerow(new_row)

def generateAddColumnSequentialNodeFile(db_name, node_name, num_schemas, shuffle, insert_loc, input_file_path, output_file_path):
    is_csv = isOutputFormatCSV(db_name)
    insert_loc = 'tail' if insert_loc is None else insert_loc
    
    if is_csv:
        generateAddColumnSequentialNodeCSVFile(db_name, num_schemas, shuffle, insert_loc, input_file_path, output_file_path)
    else:
        generateAddColumnSequentialNodeJSONFile(db_name, node_name, num_schemas, shuffle, input_file_path, output_file_path)

def generateAddColumnCombinationNodeJSONFile(db_name, node_name, num_schemas, shuffle, input_file_path, output_file_path):
    num_columns = math.ceil(math.log2(num_schemas))  # Compute number of columns required for binary representation
    schema_columns = [i for i in range(num_columns)]  # Generate column names dynamically

    # Generate all possible combinations of these columns
    schema_definitions = []
    for r in range(num_columns + 1):
        schema_definitions.extend(combinations(schema_columns, r))
    
    with open(input_file_path, 'r', newline='', encoding='utf-8') as infile, open(output_file_path, 'w', newline='', encoding='utf-8') as outfile:
        reader = csv.reader(infile, delimiter='|')
        headers = next(reader)  # Extract headers
        
        for row_index, row in enumerate(reader):
            json_object = {
                "labels": [node_name.lower()],
                "properties": {}
            }

            # Fill existing properties from the CSV
            for header, value in zip(headers, row):
                json_object["properties"][getColumnNameWithoutType(header)] = castValueToType(header, value)

            # Determine schema for this row using hash to get an index from the schema definitions
            hash_value = abs(hash(tuple(row))) % len(schema_definitions)
            selected_schema = schema_definitions[hash_value]

            # Add new properties based on selected schema (combination of columns)
            for col_idx in selected_schema:
                deterministic_random = abs(hash((row_index, col_idx))) % 101  # Include column in hash
                new_property_name = getColumnNameWithoutType(getNewColumnName(db_name, col_idx))  # Generate based on db_name, partition_index, and column
                json_object["properties"][new_property_name] = deterministic_random

            # Write the JSON object to the file
            json.dump(json_object, outfile)
            outfile.write('\n')
            
def generateAddColumnCombinationNodeFile(db_name, node_name, num_schemas, shuffle, input_file_path, output_file_path):
    is_csv = isOutputFormatCSV(db_name)
    
    if not is_csv:
        generateAddColumnCombinationNodeJSONFile(db_name, node_name, num_schemas, shuffle, input_file_path, output_file_path)

def generateSchemalessNodeFile(node_name, node_conf_yaml, db_name):
    num_schemas = node_conf_yaml['num_schemas']
    if num_schemas > 0:
        print('Schemaless data file generation for node: ' + node_name)
        mode = node_conf_yaml['mode']
        input_file_path = os.path.join(DATASET_BASE_PATH, db_name, SCALE_FOLDER, node_conf_yaml['type'], getNodeFilePrefix(db_name, node_name) + getInputDBPostfix(db_name))
        output_file_path = os.path.join(OUTPUT_BASE_PATH, CONFIG_NAME, getDBFolder(db_name), node_conf_yaml['type'], getNodeFilePrefix(db_name, node_name) + getOutputDBPostfix(db_name))
        if mode == 'add_column_seq':
            generateAddColumnSequentialNodeFile(db_name, node_name, num_schemas, node_conf_yaml['shuffle'], node_conf_yaml['insert_loc'], input_file_path, output_file_path)
        elif mode == 'add_column_comb':
            generateAddColumnCombinationNodeFile(db_name, node_name, num_schemas, node_conf_yaml['shuffle'], input_file_path, output_file_path)
        elif mode == 'remove_column':
            generateRemoveColumnNodeFile(db_name, node_name, num_schemas, node_conf_yaml['rm_cols'], node_conf_yaml['shuffle'], node_conf_yaml['null_percent'], input_file_path, output_file_path)
        else:
            print('Invalid mode for schemaless data file generation for node: ' + node_name)
        print('Schemaless data file generation for node: ' + node_name + ' completed')
        return
    else:
        print('No schemaless data file generation for node: ' + node_name)
        return

def generateSchemalessNodeFilesForDB(configuration_yaml, db_name):
    print('Generating schemaless data files for ' + db_name)
    output_path = os.path.join(OUTPUT_BASE_PATH, CONFIG_NAME, getDBFolder(db_name))
    if os.path.exists(output_path):
        print('Output path already exists. Deleting and recreating')
        os.system('rm -rf ' + output_path)
    
    os.makedirs(output_path)
    output_dynamic_path = os.path.join(output_path, 'dynamic')
    output_static_path = os.path.join(output_path, 'static')
    os.makedirs(output_dynamic_path)
    os.makedirs(output_static_path)
    
    nodes = configuration_yaml['nodes']
    # Use a process pool to execute tasks concurrently
    with ProcessPoolExecutor() as executor:
        futures = [executor.submit(generateSchemalessNodeFile, node, nodes[node], db_name) for node in nodes]

        # If you need to handle the results or exceptions, you can iterate over futures
        for future in futures:
            try:
                future.result()  # This will raise exceptions if any occurred
            except Exception as exc:
                print(f'An error occurred: {exc}')
                    
def generateSchemalessNodeFiles(configuration_yaml, db_names):
    with ProcessPoolExecutor() as executor:
        db_futures = [executor.submit(generateSchemalessNodeFilesForDB, configuration_yaml, db_name) for db_name in db_names]
        for future in db_futures:
            try:
                future.result()  # Wait for each database processing to complete
            except Exception as exc:
                print(f'An error occurred processing database: {exc}')

def createCSVSymbolicLink(input_folder_path, output_folder_path):
    input_files = [f for f in os.listdir(input_folder_path) if f.endswith('.csv')]

    for file in input_files:
        input_file_path = os.path.join(input_folder_path, file)
        output_file_path = os.path.join(output_folder_path, file)

        if not os.path.exists(output_file_path):
            os.symlink(input_file_path, output_file_path)
            print(f"Created symbolic link for {file}")
        else:
            print(f"File {file} already exists in the output folder, skipping.")

def createSymbolicLinkForNonSchemalessFiles(db_name):
    folder_types = ['dynamic', 'static']
    for folder_type in folder_types:
        input_folder_path = os.path.join(DATASET_BASE_PATH, db_name, SCALE_FOLDER, folder_type)
        output_folder_path = os.path.join(OUTPUT_BASE_PATH, CONFIG_NAME, getDBFolder(db_name), folder_type)
        if not os.path.exists(output_folder_path):
            os.makedirs(output_folder_path)
        createCSVSymbolicLink(input_folder_path, output_folder_path)
        
def createSymbolicLinkBetweenDBs(input_db_name, output_db_name):
    input_folder_path = os.path.join(OUTPUT_BASE_PATH, CONFIG_NAME, getDBFolder(input_db_name))
    output_folder_path = os.path.join(OUTPUT_BASE_PATH, CONFIG_NAME, getDBFolder(output_db_name))
    if os.path.exists(output_folder_path):
        os.system('rm -rf ' + output_folder_path)
    os.symlink(input_folder_path, output_folder_path)

#####################
### MAIN FUNCTION ###
#####################
validateArguments()
configuration_file_path = sys.argv[1]

# Get configuration file
with open(configuration_file_path, "r") as configuration_file:
    configuration_yaml = yaml.load(configuration_file, Loader=yaml.FullLoader)
    validateConfigurationFile(configuration_yaml)
    DATASET_BASE_PATH = configuration_yaml['dataset_base_path']
    OUTPUT_BASE_PATH = configuration_yaml['output_base_path']
    CONFIG_NAME = configuration_yaml['name']
    
    # Create schemaless files
    # db_names = [DUCKDB_NAME, S62_NAME, NEO4J_NAME]
    db_names = [S62_NAME]
    generateSchemalessNodeFiles(configuration_yaml, db_names)
    # createSymbolicLinkForNonSchemalessFiles(DUCKDB_NAME)
    # createSymbolicLinkBetweenDBs(DUCKDB_NAME, POSTGRESQL_NAME) # PostgreSQL have the same data as DuckDB
    # createSymbolicLinkForNonSchemalessFiles(NEO4J_NAME)
    # Kuzu
    # generateSchemalessNodeFiles(configuration_yaml, KUZU_NAME)
    # createSymbolicLinkForNonSchemalessFiles(KUZU_NAME)