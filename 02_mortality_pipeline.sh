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
echo "mortality_subdir: $config_mortality_subdir"
echo "mort_config: $config_mort_config"
echo "mort_fold_config: $config_mort_fold_config"
echo "mort_trainset_config: $config_mort_trainset_config"
echo "bcm_subdir: $config_bcm_subdir"
echo "topo_subdir: $config_topo_subdir"

# Directories
mortdir="${config_root_dir}/${config_mortality_subdir}"
bcmdir="${config_root_dir}/${config_bcm_subdir}"
topodir="${config_root_dir}/${config_topo_subdir}"
echo "Mortality directory: $mortdir"
echo "BCM directory: $bcmdir"
echo "Topo directory: $topodir"

# Step 1: Convert Mortality Data
mortality() {
    echo "Starting mortality function..."

    local output_directory="${mortdir}/tree_mortality.zarr"
    local config="${config_mort_config}"

    if [ ! -d "$mortdir" ]; then
        echo "Error: Mortality directory $mortdir does not exist."
        return 1
    fi

    echo "Output directory: $output_directory"
    echo "Using config file: $config"

    # Execute the convert_tree_mortality.py script
    echo "Executing python script to convert tree mortality data..."
    python src/convert_tree_mortality.py "$mortdir" "$config" "$output_directory"

    if [ $? -eq 0 ]; then
        echo "Mortality data conversion completed successfully."
    else
        echo "Mortality data conversion failed."
        return 1
    fi
}

# Step 2: Append Folds to Mortality Data
mortality_folds() {
    echo "Starting mortality_folds function..."

    local input_file="${mortdir}/tree_mortality.zarr"
    local output_directory="${mortdir}/tree_mortality_folds.zarr"
    local config="${config_mort_fold_config}"

    if [ ! -d "$input_file" ]; then
        echo "Error: Mortality file $input_file does not exist."
        return 1
    fi

    echo "Output directory: $output_directory"
    echo "Using config file: $config"

    # Execute the append_folds.py script
    echo "Executing python script to append folds..."
    python src/append_folds.py "$input_file" "$config" "$output_directory"

    if [ $? -eq 0 ]; then
        echo "Appending folds completed successfully."
    else
        echo "Appending folds failed."
        return 1
    fi
}

# Step 3: Construct Training Dataset
mortality_training() {
    echo "Starting mortality_training function..."

    local input_files=("${mortdir}/tree_mortality_folds.zarr" "${bcmdir}/BCMv8_indexes.zarr" "${topodir}/topo_indexes.zarr")
    local output_directory="${mortdir}/tree_mortality_training.zarr"
    local config="${config_mort_trainset_config}"

    for file in "${input_files[@]}"; do
        if [ ! -d "$file" ]; then
            echo "Error: Input file $file does not exist."
            return 1
        fi
    done

    echo "Output directory: $output_directory"
    echo "Using config file: $config"

    # Execute the construct_training_dataset.py script
    echo "Executing python script to construct training dataset..."
    python src/construct_training_dataset.py "${input_files[@]}" "$config" "$output_directory"

    if [ $? -eq 0 ]; then
        echo "Training dataset construction completed successfully."
    else
        echo "Training dataset construction failed."
        return 1
    fi
}

# Execute all steps
mortality && mortality_folds && mortality_training
