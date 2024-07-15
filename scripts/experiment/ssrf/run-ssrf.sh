#!/bin/bash
formats=("UNION" "SSRF")
distributions=("0" "1" "2")

# File path to the configuration header
config_file_path="/turbograph-v3/tbgpp-execution-engine/src/execution/physical_operator/physical_id_seek.cpp"

# Define source, target, and log directories
scale_factor=10
target_dir_base="/data/goodbye/sf${scale_factor}"
log_dir_base="/turbograph-v3/logs"

# Input parameters
queries_path="/turbograph-v3/queries/goodbye/sf${scale_factor}"
query_numbers="1-8"

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
    update_config_file ${format}
    cd /turbograph-v3/build-release && ninja

    for i in "${distributions[@]}"; do
        for query_num in $queries; do
            query_file="${queries_path}/zipf-${i}/q${query_num}.cql"
            if [ ! -f "$query_file" ]; then
                echo "Query file $query_file not found!"
                continue
            fi
            query_str=$(cat "$query_file")

            # Setup
            target_dir="${target_dir_base}/goodbye_zipf_${i}_AGGLOMERATIVE_OURS_DESCENDING"
            log_file="${log_dir}/goodbye_zipf_${i}_${format}_Q${query_num}.txt"

            # Run store
            /turbograph-v3/build-release/tbgpp-graph-store/store 200GB&
            sleep 5

            # Reun query
            timeout 3600s \
                /turbograph-v3/build-release/tbgpp-client/TurboGraph-S62 --workspace:${target_dir} --query:"$query_str" --disable-merge-join --num-iterations:4 --join-order-optimizer:exhaustive --warmup \
                >> ${log_file}

            pkill -f store
            sleep 5
        done
    done
done