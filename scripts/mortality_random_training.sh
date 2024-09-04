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

# Step 2: Construct Random Training Dataset
mortality_random_training() {
    echo "Starting mortality_random_training function..."

    local input_files=("${mort_generated_dir}/tree_mortality_random_folds.zarr" "${bcmdir}/BCMv8_indexes.zarr" "${topodir}/topo_indexes.zarr")
    local output_directory="${mort_generated_dir}/tree_mortality_random_training.zarr"
    local config="${config_mort_trainset_config}"

    for file in "${input_files[@]}"; do
        if [ ! -d "$file" ]; then
            handle_error "Input file $file does not exist."
        fi
    done

    echo "Output directory: $output_directory"
    echo "Using config file: $config"

    delete_directory "${output_directory}"

    # Execute the construct_training_dataset.py script
    echo "Executing python script to construct random training dataset..."
    python src/construct_training_dataset.py "${input_files[@]}" "$config" "$output_directory" || handle_error "Random training dataset construction failed."

    echo "Random training dataset construction completed successfully."
}

# Execute all steps
mortality_random_training
