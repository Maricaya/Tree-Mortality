#!/bin/bash

source common.sh

# Function to aggregate BCM
aggregate_bcm() {
    printf "Starting aggregate_bcm function...\n"

    local input_file="${bcmdir}/BCMv8_monthly.zarr"
    local output_directory="${bcmdir}/BCMv8_annual.zarr"
    local config="${config_agg_config}"

    delete_directory "${output_directory}"

    if [ -d "$input_file" ]; then
        printf "Aggregating BCM\n"
        python src/aggregate_bcm_v8.py "$input_file" "$config" "$output_directory"

        if [ $? -eq 0 ]; then
            printf "BCM aggregation completed successfully.\n"
        else
            printf "BCM aggregation failed.\n" >&2
            return 1
        fi
    else
        printf "Error: Monthly dataset %s does not exist.\n" "$input_file" >&2
        return 1
    fi
}

# Main execution
main() {
    print_config
    aggregate_bcm
}

main "$@"
