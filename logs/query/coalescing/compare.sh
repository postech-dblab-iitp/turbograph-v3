#!/bin/bash

dir1=$1
dir2=$2
num_queries=$3

product=1
group_counter=0

# per query
for ((i=1;i<=${num_queries};i++)); do
        file1=`(ls ${dir1}/yago_Q${i}_*)`
        file2=`(ls ${dir2}/yago_Q${i}_*)`
        exec1=`(grep -a 'Average Query Ex' ${file1} | awk '{print $5}')`
        exec2=`(grep -a 'Average Query Ex' ${file2} | awk '{print $5}')`

	if [[ -z "$exec1" || -z "$exec2" ]]; then
		echo "Query ${i} has no output"
	else
		if [[ $exec2 != 0 ]]; then
		    ratio=`echo "scale=6; ${exec1} / ${exec2}" | bc`
		    echo "Query ${i}: ${exec1} / ${exec2} = ${ratio}"
		    product=$(echo "$product * $ratio" | bc)
		else
		    echo "Query ${i}: Division by zero error"
		fi
	fi

        group_counter=$((group_counter + 1))

        if ((group_counter == 5)); then
            geomean=`echo "scale=6; e( l(${product}) / 5 )" | bc -l`
            echo "Group $(($i / 5)): Geometric Mean = ${geomean}"

            product=1
            group_counter=0
        fi
done
