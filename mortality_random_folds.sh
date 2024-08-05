#!/bin/bash

source common.sh

echo "Mortality directory: $mortdir"
echo "BCM directory: $bcmdir"
echo "Topo directory: $topodir"

# Function to handle errors
handle_error() {
    echo "Error: $1"
    exit 1
}

# Step 1: Append Random Folds to Mortality Data
mortality_random_folds() {
    echo "Starting mortality_random_folds function..."

    local input_file="${mort_generated_dir}/tree_mortality.zarr"
    local output_directory="${mort_generated_dir}/tree_mortality_random_folds.zarr"
    local config="${config_mort_rand_fold_config}"

    if [ ! -d "$input_file" ]; then
        handle_error "Mortality file $input_file does not exist."
    fi

    echo "Output directory: $output_directory"
    echo "Using config file: $config"

    delete_directory "${output_directory}"

    # Execute the append_folds.py script
    echo "Executing python script to append random folds..."
    python src/append_folds.py "$input_file" "$config" "$output_directory" || handle_error "Appending random folds failed."

    echo "Appending random folds completed successfully."
}

# Execute all steps
mortality_random_folds
