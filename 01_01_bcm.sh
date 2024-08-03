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

# Main execution
main() {
    print_config
    convert_bcm
}

main "$@"
