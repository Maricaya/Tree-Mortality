#!/bin/bash

source common.sh

# Directories
echo "Mortality directory: $mortdir"
echo "Results directory: $resultsdir"

# Function to handle errors
handle_error() {
    echo "Error: $1"
    exit 1
}

# Step: Train Model
train_model() {
    echo "Starting train_model function..."

    local base=$1
    local input_file="${mortdir}/generated/${base}_training.zarr"
    local output_file="${resultsdir}/${base}_loo.npz"

    if [ ! -d "$input_file" ]; then
        handle_error "Input file $input_file does not exist."
    fi

    echo "Input file: $input_file"
    echo "Output file: $output_file"

    delete_directory "${output_directory}"

    # Execute the train_rf_model_ray.py script
    echo "Executing python script to train model..."
    python src/train_rf_model_ray.py "$input_file" "$output_file" || handle_error "Training model failed."

    echo "Training model completed successfully for $base."
}

# Bases for training
bases=("tree_mortality" "tree_mortality_random")  # Add your bases here

# Execute train_model for each base
for base in "${bases[@]}"; do
    train_model "$base"
done
