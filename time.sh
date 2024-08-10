#!/bin/bash

# Define the list of scripts to be executed
scripts=(
  "./convert_bcm.sh"
  "./mortality_folds.sh"
  "./merge_bcm.sh"
  "./aggregate_bcm.sh"
  "./bcm_indexes.sh"
  "./topo.sh"
  "./mortality_training.sh"
)

# Output file to store execution times
output_file="sequential_execution_times.txt"

# Clear the output file at the beginning
> $output_file

# Function to log execution time of a command
log_execution_time() {
  command_name=$1
  command=$2

  start_time=$(date +%s)
  echo "Executing $command_name..."
  eval $command
  end_time=$(date +%s)
  execution_time=$((end_time - start_time))

  echo "$command_name, Execution time: $execution_time seconds" >> $output_file
}

# Function to log the runtime of sbatch jobs based on output files
log_sbatch_runtime() {
  job_name=$1
  command=$2
  output_file=$3

  # Submit the job and capture the job ID
  job_id=$(eval $command | awk '{print $4}')
  echo "Submitted job $job_name with Job ID: $job_id"

  # Track the start time
  start_time=$(date +%s)

  # Wait for the job's output file to be created and written to
  while [ ! -f "$output_file" ]; do
    sleep 1
  done

  # Wait for the job to finish writing to the output file
  tail -f --pid=$! $output_file

  # Track the end time after the job finishes
  end_time=$(date +%s)
  job_runtime=$((end_time - start_time))

  echo "$job_name, Execution time: $job_runtime seconds" >> $output_file
}

# Execute each script in sequence and track the time taken
for script in "${scripts[@]}"; do
  if [[ -x "$script" ]]; then
    # Log time for the script itself
    log_execution_time "$script" "$script"

    # Log time for `sciunit exec $script`
    log_execution_time "sciunit exec $script" "sciunit exec $script"
  else
    echo "Script: $script is not executable or not found." >> $output_file
  fi
done




# Print the contents of the output file
cat $output_file

