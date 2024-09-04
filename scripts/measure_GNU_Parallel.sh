#!/bin/bash

# Output file for recording times
output_file="parallel_execution_times.txt"

# Clear the output file if it exists
> $output_file

# Function to measure time for parallel execution with nanosecond precision
measure_time() {
  local start_time=$(date +%s%N)  # Start time in nanoseconds
  
  # Run the commands in parallel
  parallel ::: "$@"
  
  local end_time=$(date +%s%N)  # End time in nanoseconds
  local execution_time=$((end_time - start_time))
  local execution_time_sec=$(echo "scale=3; $execution_time / 1000000000" | bc)
  echo "Total parallel execution time for $step: $execution_time_sec seconds" | tee -a $output_file
}

# Step 1
step="Step 1"
measure_time "./convert_bcm.sh" "./mortality.sh"

# Step 2
step="Step 2"
measure_time "./mortality_folds.sh" "./merge_bcm.sh"

# Step 3
step="Step 3"
measure_time "./aggregate_bcm.sh"

# Step 4
step="Step 4"
measure_time "./bcm_indexes.sh" "./topo.sh"

# Step 5
step="Step 5"
measure_time "./mortality_training.sh"

echo "Execution times recorded in $output_file"

