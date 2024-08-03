#!/bin/bash

source common.sh

# Function to append BCM indexes
bcm_indexes() {
    printf "Starting bcm_indexes function...\n"

    local input_file="${bcmdir}/BCMv8_annual.zarr"
    local output_directory="${bcmdir}/BCMv8_indexes.zarr"
    local config="${config_bcm_ind_config}"

    delete_directory "${output_directory}"

    if [ -d "$input_file" ]; then
        printf "Appending BCM indexes\n"
        python src/append_climate_indexes.py "$input_file" "$config" "$output_directory"

        if [ $? -eq 0 ]; then
            printf "BCM indexes computation completed successfully.\n"
        else
            printf "BCM indexes computation failed.\n" >&2
            return 1
        fi
    else
        printf "Error: Annual dataset %s does not exist.\n" "$input_file" >&2
        return 1
    fi
}

# Main execution
main() {
    print_config
    bcm_indexes
}

main "$@"
