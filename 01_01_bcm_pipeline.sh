#!/bin/bash

source common.sh

# Function to convert BCM
convert_bcm() {
    printf "Starting convert_bcm function...\n"

    for var in "${bcm_variables[@]}"; do
        local input_file="${bcmdir}/${config_bcm_raw_subdir}/${var}"
        local output_file="${bcmdir}/${config_bcm_raw_subdir}/${var}.nc4"
        local config="${config_bcm_config}"

        if [ -d "$input_file" ]; then
            printf "Converting BCM variable: %s\n" "$var"
            python src/convert_bcm_v8.py "$input_file" "$config" "$output_file"

            if [ $? -eq 0 ]; then
                printf "Conversion of %s completed successfully.\n" "$var"
            else
                printf "Conversion of %s failed.\n" "$var" >&2
                return 1
            fi
        else
            printf "Error: Input directory %s does not exist.\n" "$input_file" >&2
            return 1
        fi
    done
}

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
    convert_bcm && merge_bcm && aggregate_bcm
}

main "$@"
