#!/bin/bash

# Configuration
configfile="config/snakemake.yml"

# Function to parse YAML (simplified)
parse_yaml() {
    local prefix=$2
    local s='[[:space:]]*'
    local w='[a-zA-Z0-9_]*'
    local fs=$(echo @|tr @ '\034')
    sed -ne "s|^\($s\):|\1|" \
         -e "s|^\($s\)\($w\)$s:$s\"\(.*\)\"$s\$|$prefix\2=\3|p" \
         -e "s|^\($s\)\($w\)$s:$s\(.*\)$s\$|$prefix\2=\3|p" $1
}

# Load configurations from YAML
eval $(parse_yaml $configfile "config_")

# Print loaded configuration
echo "Loaded configuration from $configfile:"
echo "root_dir: $config_root_dir"
echo "results_subdir: $config_results_subdir"
echo "mortality_subdir: $config_mortality_subdir"

# Directories
mortdir="${config_root_dir}/${config_mortality_subdir}"
resultsdir="${config_root_dir}/${config_results_subdir}"
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
