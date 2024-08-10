#!/bin/bash

#SBATCH --job-name=audit
#SBATCH --output=slurm-audit.out
#SBATCH --error=slurm-audit.out
#SBATCH --time=02:00:00
#SBATCH --ntasks-per-node=1

# Log the start time of the job
echo "Job started at: $(date)" >> timing.log

# Execute each step and log the times
for step in 1 2 3 4 5
do
    start_time=$(date +%s)
    tail ./step${step}.sh | parallel -j4
    end_time=$(date +%s)
    elapsed_time=$(($end_time - $start_time))
    echo "step$step  finished in $elapsed_time seconds" >> timing.log
done

# Log the end time of the job
echo "Job ended at: $(date)" >> timing.log
