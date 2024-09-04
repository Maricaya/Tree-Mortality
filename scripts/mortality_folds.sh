#!/bin/bash

source common.sh

# Step : Append Folds to Mortality Data
mortality_folds() {
    printf "Starting mortality_folds function...\n"

    local input_file="${mort_generated_dir}/tree_mortality.zarr"
    local output_directory="${mort_generated_dir}/tree_mortality_folds.zarr"
    local config="${config_mort_fold_config}"

    delete_directory "${output_directory}"

    if [[ ! -d "$input_file" ]]; then
        printf "Error: Mortality file %s does not exist.\n" "$input_file" >&2
        return 1
    fi

    printf "Output directory: %s\n" "$output_directory"
    printf "Using config file: %s\n" "$config"

    # Execute the append_folds.py script
    printf "Executing python script to append folds...\n"
    python src/append_folds.py "$input_file" "$config" "$output_directory"

    if [[ $? -eq 0 ]]; then
        printf "Appending folds completed successfully.\n"
    else
        printf "Appending folds failed.\n" >&2
        return 1
    fi
}

# Main execution
main() {
    mortality_folds
}

main "$@"