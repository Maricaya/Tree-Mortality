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

# Step 3: Aggregate Projection Data
aggregate_projection() {
    echo "Starting aggregate_projection function..."

    local model=$1
    local scenario=$2
    local input_file="${projdir}/${model}/${scenario}.zarr"
    local output_directory="${projdir}/${model}/${scenario}_annual.zarr"
    local config="${config_agg_config}"

    if [ ! -d "$input_file" ]; then
        handle_error "Input file $input_file does not exist."
    fi

    echo "Input file: $input_file"
    echo "Output directory: $output_directory"
    echo "Using config file: $config"

    delete_directory "${output_directory}"
    # Execute the aggregate_bcm_v8.py script
    echo "Executing python script to aggregate projections..."
    python src/aggregate_bcm_v8.py "$input_file" "$config" "$output_directory" || handle_error "Aggregation of projections failed."

    echo "Aggregation of projections completed successfully for $model - $scenario."
}

# Models and scenarios
model="GFDL-CM3"  # Add your models here
scenario="RCP45"  # Add your scenarios here

aggregate_projection "$model" "$scenario"
