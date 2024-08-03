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

# Step 2: Append Folds to Mortality Data
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

# Step 3: Construct Training Dataset
mortality_training() {
    printf "Starting mortality_training function...\n"

    local input_files=("${mort_generated_dir}/tree_mortality_folds.zarr" "${bcmdir}/BCMv8_indexes.zarr" "${topodir}/topo_indexes.zarr")
    local output_directory="${mort_generated_dir}/tree_mortality_training.zarr"
    local config="${config_mort_trainset_config}"

    delete_directory "${output_directory}"

    for file in "${input_files[@]}"; do
        if [[ ! -d "$file" ]]; then
            printf "Error: Input file %s does not exist.\n" "$file" >&2
            return 1
        fi
    done

    printf "Output directory: %s\n" "$output_directory"
    printf "Using config file: %s\n" "$config"

    # Execute the construct_training_dataset.py script
    printf "Executing python script to construct training dataset...\n"
    python src/construct_training_dataset.py "${input_files[@]}" "$config" "$output_directory"

    if [[ $? -eq 0 ]]; then
        printf "Training dataset construction completed successfully.\n"
    else
        printf "Training dataset construction failed.\n" >&2
        return 1
    fi
}

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
    print_config
    mortality && mortality_folds && mortality_training && nonzero_mortality
}

main "$@"
