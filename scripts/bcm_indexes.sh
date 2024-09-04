#!/bin/bash

source common.sh

# Function to append BCM indexes
bcm_indexes() {
    echo "Starting bcm_indexes function..."

    local input_file="${bcmdir}/BCMv8_annual.zarr"
    local output_directory="${bcmdir}/BCMv8_indexes.zarr"
    local config="${config_bcm_ind_config}"

    delete_directory "${output_directory}"

    if [ -d "$input_file" ]; then
        echo "Appending BCM indexes"
        python src/append_climate_indexes.py "$input_file" "$config" "$output_directory"

        if [ $? -eq 0 ]; then
            echo "BCM indexes computation completed successfully."
        else
            echo "BCM indexes computation failed."
            return 1
        fi
    else
        echo "Error: Annual dataset $input_file does not exist."
        return 1
    fi
}

bcm_indexes