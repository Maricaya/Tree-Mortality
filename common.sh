#!/bin/bash

# Configuration
CONFIGFILE="config/snakemake.yml"
SCIUNIT_PROJECT_NAME="tree-debug"

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
eval $(parse_yaml $CONFIGFILE "config_")

# Function to delete a directory if it exists
delete_directory() {
    local target_directory="~/sciunit/${SCIUNIT_PROJECT_NAME}/cde-package/cde-root${1}"
    target_directory=$(eval echo ${target_directory})

    if [ -d "${target_directory}" ]; then
        find "${target_directory}" -mindepth 1 -exec rm -rf {} +
        printf "Deleted contents of directory: %s\n" "${target_directory}"
        rm -rf "${target_directory}"
        printf "Deleted directory: %s\n" "${target_directory}"
    else
        printf "Directory does not exist: %s\n" "${target_directory}" >&2
    fi
}

# Print loaded configuration
print_config() {
    printf "Loaded configuration from %s:\n" "$CONFIGFILE"
    printf "root_dir: %s\n" "$config_root_dir"
    printf "bcm_subdir: %s\n" "$config_bcm_subdir"
    printf "bcm_raw_subdir: %s\n" "$config_bcm_raw_subdir"
    printf "bcm_config: %s\n" "$config_bcm_config"
    printf "agg_config: %s\n" "$config_agg_config"
    printf "bcm_ind_config: %s\n" "$config_bcm_ind_config"
}

# Directories
bcmdir="${config_root_dir}/${config_bcm_subdir}"
bcm_variables=(pet ppt)
