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
echo "projdir: $config_projections_subdir"
echo "bcm_subdir: $config_bcm_subdir"
echo "bcm_config: $config_bcm_config"
echo "bcm_ind_config: $config_bcm_ind_config"

# Directories
projdir="${config_root_dir}/${config_projections_subdir}"
bcmdir="${config_root_dir}/${config_bcm_subdir}"
echo "Projections directory: $projdir"
echo "BCM directory: $bcmdir"

# Function to handle errors
handle_error() {
    echo "Error: $1"
    exit 1
}

# Step 1: Convert Projection Data from ZIP to netCDF4
convert_projection() {
    echo "Starting convert_projection function..."

    local model=$1
    local scenario=$2
    local var=$3
    local input_file="${projdir}/${model}/${scenario}/${var}_${model}_${scenario}.zip"
    local output_file="${projdir}/${model}/${scenario}/${var}_${model}_${scenario}.nc4"
    local config="${config_bcm_config}"

    if [ ! -f "$input_file" ]; then
        handle_error "Input file $input_file does not exist."
    fi

    echo "Input file: $input_file"
    echo "Output file: $output_file"
    echo "Using config file: $config"

    # Execute the convert_projections.py script
    echo "Executing python script to convert projections..."
    python src/convert_projections.py "$input_file" "$config" "$output_file" || handle_error "Conversion of projections failed."

    echo "Conversion of projections completed successfully for $model - $scenario - $var."
}

# Step 2: Merge Projection Data
merge_projection() {
    echo "Starting merge_projection function..."

    local model=$1
    local scenario=$2
    local input_files=()
    for var in "${bcm_variables[@]}"; do
        input_files+=("${projdir}/${model}/${scenario}/${var}_${model}_${scenario}.nc4")
    done
    local output_directory="${projdir}/${model}/${scenario}.zarr"
    local config="${config_bcm_config}"

    echo "Input files: ${input_files[*]}"
    echo "Output directory: $output_directory"
    echo "Using config file: $config"

    # Execute the merge_projections.py script
    echo "Executing python script to merge projections..."
    python src/merge_bcm.py "${input_files[@]}" "$config" "$output_directory" || handle_error "Merging of projections failed."

    echo "Merging of projections completed successfully for $model - $scenario."
}

# Step 3: Append Projection Indexes
projection_indexes() {
    echo "Starting projection_indexes function..."

    local model=$1
    local scenario=$2
    local input_file="${projdir}/${model}/${scenario}_annual.zarr"
    local output_directory="${projdir}/${model}/${scenario}_indexes.zarr"
    local config="${config_bcm_ind_config}"
    local ref="${bcmdir}/BCMv8_annual.zarr"

    if [ ! -d "$input_file" ]; then
        handle_error "Input file $input_file does not exist."
    fi

    echo "Output directory: $output_directory"
    echo "Using config file: $config"

    # Execute the append_climate_indexes.py script
    echo "Executing python script to append climate indexes..."
    python src/append_climate_indexes.py "$input_file" "$config" "$output_directory" -r "$ref" || handle_error "Appending of projection indexes failed."

    echo "Appending of projection indexes completed successfully for $model - $scenario."
}

# Models and scenarios
models=("GFDL-CM3")  # Add your models here
scenarios=("RCP45")  # Add your scenarios here
bcm_variables=("pet" "ppt")  # Add your variables here

# Execute steps for each combination of model, scenario, and variable
for model in "${models[@]}"; do
    for scenario in "${scenarios[@]}"; do
        for var in "${bcm_variables[@]}"; do
            convert_projection "$model" "$scenario" "$var"
        done
        merge_projection "$model" "$scenario"
#        projection_indexes "$model" "$scenario"
    done
done
