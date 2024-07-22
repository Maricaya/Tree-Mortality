#!/bin/bash

# Configuration
configfile="config/snakemake.yml"

# Function to parse YAML (simplified)
parse_yaml() {
    local prefix=$2
    local s='[[:space:]]*'
    local w='[a-zA-Z0-9_]*'
    local fs=$(echo @|tr @ '\034')
    sed -ne "s|^\($s\):|\1|" \
         -e "s|^\($s\)\($w\)$s:$s\"\(.*\)\"$s\$|$prefix\2=\3|p" \
         -e "s|^\($s\)\($w\)$s:$s\(.*\)$s\$|$prefix\2=\3|p" $1
}

# Load configurations from YAML
eval $(parse_yaml $configfile "config_")

# Print loaded configuration
echo "Loaded configuration from $configfile:"
echo "root_dir: $config_root_dir"
echo "bcm_subdir: $config_bcm_subdir"
echo "bcm_raw_subdir: $config_bcm_raw_subdir"
echo "bcm_config: $config_bcm_config"
echo "agg_config: $config_agg_config"
echo "bcm_ind_config: $config_bcm_ind_config"

# Directories
bcmdir="${config_root_dir}/${config_bcm_subdir}"
echo "BCM directory: $bcmdir"

# Step 1: Convert BCM
convert_bcm() {
    echo "Starting convert_bcm function..."

    bcm_variables=(aet cwd pck pet ppt rch run str tmn tmx)
    for var in "${bcm_variables[@]}"; do
        input_file="${bcmdir}/${config_bcm_raw_subdir}/${var}"
        output_file="${bcmdir}/${config_bcm_raw_subdir}/${var}.nc4"
        config="${config_bcm_config}"

        if [ -d "$input_file" ]; then
            echo "Converting BCM variable: $var"
            python src/convert_bcm_v8.py "$input_file" "$config" "$output_file"

            if [ $? -eq 0 ]; then
                echo "Conversion of $var completed successfully."
            else
                echo "Conversion of $var failed."
                return 1
            fi
        else
            echo "Error: Input directory $input_file does not exist."
            return 1
        fi
    done
}

# Step 2: Merge BCM
merge_bcm() {
    echo "Starting merge_bcm function..."

    local input_files=()
    for var in "${bcm_variables[@]}"; do
        input_files+=("${bcmdir}/${config_bcm_raw_subdir}/${var}.nc4")
    done

    local output_directory="${bcmdir}/BCMv8_monthly.zarr"
    local config="${config_bcm_config}"

    echo "Output directory: $output_directory"
    echo "Using config file: $config"

    python src/merge_bcm.py "${input_files[@]}" "$config" "$output_directory"

    if [ $? -eq 0 ]; then
        echo "BCM merge completed successfully."
    else
        echo "BCM merge failed."
        return 1
    fi
}

# Step 3: Aggregate BCM
aggregate_bcm() {
    echo "Starting aggregate_bcm function..."

    local input_file="${bcmdir}/BCMv8_monthly.zarr"
    local output_directory="${bcmdir}/BCMv8_annual.zarr"
    local config="${config_agg_config}"

    if [ -d "$input_file" ]; then
        echo "Aggregating BCM"
        python src/aggregate_bcm_v8.py "$input_file" "$config" "$output_directory"

        if [ $? -eq 0 ]; then
            echo "BCM aggregation completed successfully."
        else
            echo "BCM aggregation failed."
            return 1
        fi
    else
        echo "Error: Monthly dataset $input_file does not exist."
        return 1
    fi
}

# Step 4: Append BCM Indexes
bcm_indexes() {
    echo "Starting bcm_indexes function..."

    local input_file="${bcmdir}/BCMv8_annual.zarr"
    local output_directory="${bcmdir}/BCMv8_indexes.zarr"
    local config="${config_bcm_ind_config}"

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

# Execute all steps
convert_bcm && merge_bcm && aggregate_bcm && bcm_indexes
