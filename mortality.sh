#!/bin/bash

source common.sh

# Step 1: Convert Mortality Data
mortality() {
    printf "Starting mortality function...\n"

    local output_directory="${mort_generated_dir}/tree_mortality.zarr"
    local config="${config_mort_config}"

    if [[ ! -d "$mortdir" ]]; then
        printf "Error: Mortality directory %s does not exist.\n" "$mortdir" >&2
        return 1
    fi

    delete_directory "${output_directory}"

    printf "Output directory: %s\n" "$output_directory"
    printf "Using config file: %s\n" "$config"

    # Execute the convert_tree_mortality.py script
    printf "Executing python script to convert tree mortality data...\n"
    python src/convert_tree_mortality.py "$mortdir" "$config" "$output_directory"

    if [[ $? -eq 0 ]]; then
        printf "Mortality data conversion completed successfully.\n"
    else
        printf "Mortality data conversion failed.\n" >&2
        return 1
    fi
}

# Main execution
main() {
    mortality
}

main "$@"