#!/bin/bash

source common.sh

# Step 4: Filter Non-Zero Mortality Data
nonzero_mortality() {
    printf "Starting nonzero_mortality function...\n"

    local input_file="${mort_generated_dir}/tree_mortality_training.zarr"
    local output_directory="${mort_generated_dir}/tree_mortality_training_nonzero.zarr"
    local config="${config_mort_trainset_config}"

    delete_directory "${output_directory}"

    if [[ ! -d "$input_file" ]]; then
        printf "Error: Training dataset %s does not exist.\n" "$input_file" >&2
        return 1
    fi

    printf "Output directory: %s\n" "$output_directory"
    printf "Using config file: %s\n" "$config"

    # Execute the filter_zero_values.py script
    printf "Executing python script to filter zero values...\n"
    python src/filter_zero_values.py "$input_file" "$config" "$output_directory"

    if [[ $? -eq 0 ]]; then
        printf "Non-zero mortality data filtering completed successfully.\n"
    else
        printf "Non-zero mortality data filtering failed.\n" >&2
        return 1
    fi
}

# Main execution
main() {
    nonzero_mortality
}

main "$@"