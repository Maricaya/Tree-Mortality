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

# Step 4: Append Projection Indexes
projection_indexes() {
    echo "Starting projection_indexes function..."

    local model=$1
    local scenario=$2
    local input_file="${projdir}/${model}/${scenario}_annual.zarr"
    local output_directory="${projdir}/${model}/${scenario}_indexes.zarr"
    local config="${config_bcm_ind_config}"
    local ref="${bcmdir}/BCMv8_annual.zarr"

    if [ ! -d "$input_file" ]; then
        handle_error "Input file $input_file does not exist."
    fi

    echo "Output directory: $output_directory"
    echo "Using config file: $config"

    delete_directory "${output_directory}"

    # Execute the append_climate_indexes.py script
    echo "Executing python script to append climate indexes..."
    python src/append_climate_indexes.py "$input_file" "$config" "$output_directory" -r "$ref" || handle_error "Appending of projection indexes failed."

    echo "Appending of projection indexes completed successfully for $model - $scenario."
}

# Models and scenarios
model="GFDL-CM3"  # Add your models here
scenario="RCP45"  # Add your scenarios here

projection_indexes "$model" "$scenario"
