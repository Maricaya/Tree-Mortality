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

# Step 1: Convert Projection Data from ZIP to netCDF4
convert_projection() {
    echo "Starting convert_projection function..."

    local model=$1
    local scenario=$2
    local var=$3
    local input_file="${projdir}/${model}/${scenario}/${var}_${model}_${scenario}.zip"
    local output_file="${projdir}/${model}/${scenario}/${var}_${model}_${scenario}.nc4"
    local config="${config_bcm_config}"

    if [ ! -f "$input_file" ]; then
        handle_error "Input file $input_file does not exist."
    fi

    echo "Input file: $input_file"
    echo "Output file: $output_file"
    echo "Using config file: $config"

    # Execute the convert_projections.py script
    echo "Executing python script to convert projections..."
    python src/convert_projections.py "$input_file" "$config" "$output_file" || handle_error "Conversion of projections failed."

    echo "Conversion of projections completed successfully for $model - $scenario - $var."
}

# Models and scenarios
model="GFDL-CM3"  # Add your models here
scenario="RCP45"  # Add your scenarios here

# Execute steps for each combination of model, scenario, and variable
for var in "${bcm_variables[@]}"; do
    convert_projection "$model" "$scenario" "$var"
done

