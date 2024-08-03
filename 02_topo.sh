#!/bin/bash

source common.sh

# Directories
echo "Mortality directory: $mortdir"
echo "BCM directory: $bcmdir"
echo "Topo directory: $topodir"

# Function to handle errors
handle_error() {
    echo "Error: $1"
    exit 1
}

# Step 8: Generate Topographic Features
# todo 先不做，因为有 R
#topo_features() {
#    echo "Starting topo_features function..."
#
#    local demfile="${topodir}/${config_topobase}"
#    local output_directory="${topodir}/generated"
#
#    if [ ! -f "$demfile" ]; then
#        handle_error "DEM file $demfile does not exist."
#    fi
#
#    echo "Output directory: $output_directory"
#
#    # Execute the raster_topography_indexes.r script
#    echo "Executing R script to generate topographic features..."
#    Rscript r/raster_topography_indexes.r -i "$demfile" -o "$output_directory" || handle_error "Topographic features generation failed."
#
#    echo "Topographic features generation completed successfully."
#}

# Step 9: Merge Topographic Features with Annual Dataset
topo() {
    echo "Starting topo function..."

    local features="${topodir}/generated"
    local bcm="${bcmdir}/BCMv8_annual.zarr"
    local output_directory="${topodir}/topo_indexes.zarr"
    local config="${config_topo_config}"

    if [ ! -d "$features" ]; then
        handle_error "Features directory $features does not exist."
    fi
    if [ ! -d "$bcm" ]; then
        handle_error "BCM directory $bcm does not exist."
    fi

    echo "Output directory: $output_directory"
    echo "Using config file: $config"

    delete_directory "${output_directory}"

    # Execute the merge_topo_features.py script
    echo "Executing python script to merge topographic features..."
    python src/merge_topo_features.py "$features"/*.nc "$bcm" "$config" "$output_directory" || handle_error "Merging topographic features failed."

    echo "Merging topographic features completed successfully."
}

# Execute the steps
#topo_features
topo
