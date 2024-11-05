#!/bin/bash

TARGET_DIR=$1
PATTERN="Average Query Exec"

#read -p "Enter the T value (e.g., tree or chain): " T_VALUE
T_VALUE=$1

found_any_pattern=false

for file in "$TARGET_DIR"/yago_Q_*_AGGLOMERATIVE_OURS_DESCENDING.txt; do
    filename=$(basename "$file")
    pattern=$(echo "$filename" | sed -E 's/yago_Q_(.*)_AGGLOMERATIVE_OURS_DESCENDING.txt/\1/')

    DESCENDING_FILE="$file"
    IN_STORAGE_FILE="$TARGET_DIR/yago_Q_${pattern}_AGGLOMERATIVE_OURS_DESCENDING_IN_STORAGE.txt"
    SINGLECLUSTER_FILE="$TARGET_DIR/yago_Q_${pattern}_SINGLECLUSTER_OURS_DESCENDING.txt"

    if [[ ! -f "$IN_STORAGE_FILE" || ! -f "$SINGLECLUSTER_FILE" ]]; then
        echo "Skipping pattern $pattern: missing required files."
        continue
    fi

    descending_value=$(grep -a "$PATTERN" "$DESCENDING_FILE" | awk '{print $5}')
    in_storage_value=$(grep -a "$PATTERN" "$IN_STORAGE_FILE" | awk '{print $5}')
    singlecluster_value=$(grep -a "$PATTERN" "$SINGLECLUSTER_FILE" | awk '{print $5}')

    python3 - <<END
import math

desc_val = float("${descending_value}")
in_storage_val = float("${in_storage_value}")
singlecluster_val = float("${singlecluster_value}")

in_storage_ratio = in_storage_val / desc_val
singlecluster_ratio = singlecluster_val / desc_val

with open("/tmp/in_storage_ratios.txt", "a") as f:
    f.write(f"{in_storage_ratio}\n")

with open("/tmp/singlecluster_ratios.txt", "a") as f:
    f.write(f"{singlecluster_ratio}\n")
END

    found_any_pattern=true
done

if [ "$found_any_pattern" = true ]; then
    python3 - <<END
import math

with open("/tmp/in_storage_ratios.txt") as f:
    in_storage_ratios = [float(line.strip()) for line in f]

with open("/tmp/singlecluster_ratios.txt") as f:
    singlecluster_ratios = [float(line.strip()) for line in f]

def geometric_mean(ratios):
    product = math.prod(ratios)
    return product ** (1 / len(ratios))

in_storage_geomean = geometric_mean(in_storage_ratios)
singlecluster_geomean = geometric_mean(singlecluster_ratios)

print(f"T_VALUE (${T_VALUE}): Overall Geometric Mean of Ratios")
print(f"  AGGLOMERATIVE_OURS_DESCENDING_IN_STORAGE = {in_storage_geomean:.4f}")
print(f"  SINGLECLUSTER_OURS_DESCENDING = {singlecluster_geomean:.4f}")
END

    rm -f /tmp/in_storage_ratios.txt /tmp/singlecluster_ratios.txt
else
    echo "No valid patterns found with all required files."
fi

