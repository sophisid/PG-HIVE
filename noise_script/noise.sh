#!/bin/bash

# Define an array of datasets
datasets=("LDBC" "FIB" "MB6")

# Define an array of corruption percentages
percentages=(0 10 20 30 40 50)

# Base input and output paths
input_base="../datasets/"
output_base="../noisy_datasets/"

# Loop over datasets
for dataset in "${datasets[@]}"; do
    # Loop over percentages
    for percent in "${percentages[@]}"; do
        # Construct the input and output paths
        input_folder="${input_base}${dataset}/"
        output_folder="${output_base}${dataset}/corrupted${percent}/"

        # Run the Python script with the constructed paths
        echo "Processing ${dataset} with ${percent}% corruption..."
        python3 remove_cells.py "$input_folder" "$percent" "|" "$output_folder"
    done
done

echo "All datasets processed."
