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

# Step 3: Filter Non-Zero Random Mortality Data
nonzero_random_mortality() {
    echo "Starting nonzero_random_mortality function..."

    local input_file="${mort_generated_dir}/tree_mortality_random_training.zarr"
    local output_directory="${mort_generated_dir}/tree_mortality_random_training_nonzero.zarr"
    local config="${config_mort_trainset_config}"

    if [ ! -d "$input_file" ]; then
        handle_error "Random training dataset $input_file does not exist."
    fi

    echo "Output directory: $output_directory"
    echo "Using config file: $config"

    delete_directory "${output_directory}"

    # Execute the filter_zero_values.py script
    echo "Executing python script to filter zero values from random training dataset..."
    python src/filter_zero_values.py "$input_file" "$config" "$output_directory" || handle_error "Non-zero random mortality data filtering failed."

    echo "Non-zero random mortality data filtering completed successfully."
}

# Execute all steps
nonzero_random_mortality
