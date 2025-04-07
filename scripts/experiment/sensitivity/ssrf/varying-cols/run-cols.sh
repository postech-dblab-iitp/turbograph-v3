#!/bin/bash

formats=("UNION" "SSRF")
# formats=("SSRF")
# cols=("1" "2" "3" "4" "5")
# cols=("3" "4" "5")

# File path to the configuration header
config_file_path="/turbograph-v3/src/execution/execution/physical_operator/physical_id_seek.cpp"

# Define source, target, and log directories
target_dir_base="/data/dbpedia/"
log_dir_base="/turbograph-v3/logs"

# Input parameters
queries_base_path="/turbograph-v3/queries/kg/dbpedia-hops/"
query_numbers="1;2"

# Function to update the configuration file with new values
update_config_file() {
    local format=$1

    # Use sed to update the configuration file
    if [ "${format}" == "UNION" ]; then
        sed -i "s/static bool unionall_forced = .*/static bool unionall_forced = true;/" ${config_file_path}
    else
        sed -i "s/static bool unionall_forced = .*/static bool unionall_forced = false;/" ${config_file_path}
    fi
}

# Get current date and time for log directory
current_datetime=$(date +"%Y-%m-%d")
log_dir="${log_dir_base}/format/${current_datetime}"
mkdir -p ${log_dir}

# Function to parse query numbers
parse_query_numbers() {
    if [[ $1 == *-* ]]; then
        IFS='-' read -ra RANGE <<< "$1"
        for i in $(seq ${RANGE[0]} ${RANGE[1]}); do
            echo $i
        done
    elif [[ $1 == *';'* ]]; then
        IFS=';' read -ra NUMS <<< "$1"
        for i in "${NUMS[@]}"; do
            echo $i
        done
    else
        echo $1
    fi
}

# Parse query numbers
queries=$(parse_query_numbers $query_numbers)

for format in "${formats[@]}"; do
    for col in "${cols[@]}"; do
        update_config_file ${format}
        cd /turbograph-v3/build-release && ninja
        cd -

        output_file="format_${col}cols_${format}.csv"
        echo "QueryNumber,CompileTime,QueryExecutionTime,EndtoEndTime" > $output_file

        queries_path="${queries_base_path}/${col}"

        for query_num in $queries; do
            query_file="${queries_path}/q${query_num}.cql"
            if [ ! -f "$query_file" ]; then
                echo "Query file $query_file not found!"
                continue
            fi
            query_str=$(cat "$query_file")

            # Setup
            target_dir="${target_dir_base}/dbpedia_AGGLOMERATIVE_OURS_DESCENDING"
            log_file="${log_dir}/format_${col}cols_${format}_Q${query_num}.txt"

            # Run query
            output_str=$(timeout 3600s \
                /turbograph-v3/build-release/tools/client \
                --standalone \
                --slient \
                --workspace ${target_dir} \
                --query "${query_str}" \
                --disable-merge-join \
                --iterations 3 \
                --join-order-optimizer exhaustive \
                --warmup)

            # Output to log file
            echo "$output_str" >> "$log_file"

            # Extract times
            compile_time=$(echo "$output_str" | grep -oP 'Average Compile Time: \K[0-9.]+' | awk '{printf "%.2f", $1}')
            exec_time=$(echo "$output_str" | grep -oP 'Average Query Execution Time: \K[0-9.]+' | awk '{printf "%.2f", $1}')
            end_to_end_time=$(echo "$output_str" | grep -oP 'Average End to End Time: \K[0-9.]+' | awk '{printf "%.2f", $1}')


            echo "Q$query_num, $compile_time, $exec_time, $end_to_end_time" >> "$output_file"
        done
    done
done