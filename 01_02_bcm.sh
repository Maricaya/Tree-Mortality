#!/bin/bash

source common.sh

# Function to merge BCM
merge_bcm() {
    printf "Starting merge_bcm function...\n"

    local input_files=()
    for var in "${bcm_variables[@]}"; do
        input_files+=("${bcmdir}/${config_bcm_raw_subdir}/${var}.nc4")
    done

    local output_directory="${bcmdir}/BCMv8_monthly.zarr"
    local config="${config_bcm_config}"

    delete_directory "${output_directory}"

    printf "Output directory: %s\n" "$output_directory"
    printf "Using config file: %s\n" "$config"

    python src/merge_bcm.py "${input_files[@]}" "$config" "$output_directory"

    if [ $? -eq 0 ]; then
        printf "BCM merge completed successfully.\n"
    else
        printf "BCM merge failed.\n" >&2
        return 1
    fi
}

# Main execution
main() {
    print_config
    merge_bcm
}

main "$@"
