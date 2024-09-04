#!/bin/bash

# Configuration
CONFIGFILE="config/snakemake.yml"
# todo auto
SCIUNIT_PROJECT_NAME="tree-mortality"

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

delete_files_and_directories() {
    local dir="$1"
 #   echo "Processing directory: $dir"
    
    # Iterate over all files and directories in the current directory, including hidden ones
    for item in "$dir"/* "$dir"/.[!.]* "$dir"/..?*; do
        # Skip if no such file or directory
        [ -e "$item" ] || continue
        
        if [ -d "$item" ]; then
            # If the item is a directory, call the function recursively
            delete_files_and_directories "$item"
        else
            # If the item is a file, delete it
#            echo "Deleting file: $item"
            rm -f "$item"
        fi
    done

    # Attempt to delete the current directory
  #  echo "Deleting directory: $dir"
    rmdir "$dir"
}

delete_directory() {
    local sub_path="${1}"
    local base_directory="$HOME/sciunit/${SCIUNIT_PROJECT_NAME}/cde-package/cde-root"
  #  echo "---= Base directory: ${base_directory}"
  #  echo "---= Sub path: ${sub_path}"

    local target_directory="${base_directory}${sub_path}"
  #  echo "---= Target directory before evaluation: ${target_directory}"
    target_directory=$(eval echo ${target_directory})
  #  echo "==== Target directory after evaluation: ${target_directory}"

    if [ -d "${sub_path}" ]; then 
 	delete_files_and_directories "${sub_path}"
 #       if [ $? -eq 0 ]; then
#            printf "==== sub_path  Deleted directory: %s\n" "${sub_path}"
#        else
#            printf "==== sub_path  Failed to delete directory: %s\n" "${sub_path}"
#        fi
#    else
#        printf "==== sub_path  Directory does not exist: %s\n" "${target_directory}" >&2
    fi

    if [ -d "${target_directory}" ]; then
  #      echo "===== Directory exists: ${target_directory}"
  #      echo "==== Running: /bin/rm -rf ${target_directory}"
   
	delete_files_and_directories "${target_directory}"

#	if [ $? -eq 0 ]; then
#            printf "==== Deleted directory: %s\n" "${target_directory}"
#        else
#            printf "==== Failed to delete directory: %s\n" "${target_directory}"
#        fi
#    else
#        printf "==== Directory does not exist: %s\n" "${target_directory}" >&2
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
#bcm_variables=(aet cwd pck pet ppt rch run str tmn tmx)  # Add your variables here
bcm_variables=(aet cwd pck pet ppt)
topodir="${config_root_dir}/${config_topo_subdir}"
projdir="${config_root_dir}/${config_projections_subdir}"
resultsdir="${config_root_dir}/${config_results_subdir}"
