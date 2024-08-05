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
mortality_random_folds && mortality_random_training && nonzero_random_mortality