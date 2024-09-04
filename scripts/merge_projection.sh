#!/bin/bash

source common.sh

# Directories
echo "Projections directory: $projdir"
echo "BCM directory: $bcmdir"

# Function to handle errors
handle_error() {
    echo "Error: $1"
    exit 1
}

# Step 2: Merge Projection Data
merge_projection() {
    echo "Starting merge_projection function..."

    local model=$1
    local scenario=$2
    local input_files=()
    for var in "${bcm_variables[@]}"; do
        input_files+=("${projdir}/${model}/${scenario}/${var}_${model}_${scenario}.nc4")
    done
    local output_directory="${projdir}/${model}/${scenario}.zarr"
    local config="${config_bcm_config}"

    echo "Input files: ${input_files[*]}"
    echo "Output directory: $output_directory"
    echo "Using config file: $config"

    delete_directory "${output_directory}"

    # Execute the merge_projections.py script
    echo "Executing python script to merge projections..."
    python src/merge_bcm.py "${input_files[@]}" "$config" "$output_directory" || handle_error "Merging of projections failed."

    echo "Merging of projections completed successfully for $model - $scenario."
}

# Models and scenarios
model="GFDL-CM3"  # Add your models here
scenario="RCP45"  # Add your scenarios here

merge_projection "$model" "$scenario"

