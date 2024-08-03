#!/bin/bash

# Configuration
CONFIGFILE="config/snakemake.yml"
# todo auto
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
        /usr/bin/find "${target_directory}" -mindepth 1 -exec /bin/rm -rf {} +
#        printf "Deleted contents of directory: %s\n" "${target_directory}"
        /bin/rm -rf "${target_directory}"
#        printf "Deleted directory: %s\n" "${target_directory}"
#    else
#        printf "Directory does not exist: %s\n" "${target_directory}" >&2
    fi
}

# Print loaded configuration
print_config() {
    printf "Loaded configuration from %s:\n" "$CONFIGFILE"
    printf "root_dir: %s\n" "$config_root_dir"
    printf "mortality_subdir: %s\n" "$config_mortality_subdir"
    printf "mort_config: %s\n" "$config_mort_config"
    printf "mort_fold_config: %s\n" "$config_mort_fold_config"
    printf "mort_trainset_config: %s\n" "$config_mort_trainset_config"
    printf "bcm_subdir: %s\n" "$config_bcm_subdir"
    printf "bcm_ind_config: %s\n" "$config_bcm_ind_config"
    printf "topo_subdir: %s\n" "$config_topo_subdir"
    echo "topobase: $config_topobase"
    echo "topo_config: $config_topo_config"
    echo "projdir: $config_projections_subdir"
    echo "bcm_config: $config_bcm_config"
    echo "agg_config: $config_agg_config"
    echo "results_subdir: $config_results_subdir"
}

# Directories
mortdir="${config_root_dir}/${config_mortality_subdir}"
mort_generated_dir="${mortdir}/generated"
bcmdir="${config_root_dir}/${config_bcm_subdir}"
bcm_variables=(pet ppt)
topodir="${config_root_dir}/${config_topo_subdir}"
projdir="${config_root_dir}/${config_projections_subdir}"
resultsdir="${config_root_dir}/${config_results_subdir}"
