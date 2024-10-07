#!/bin/bash

#SBATCH --job-name=audit
#SBATCH --output=audit.out
#SBATCH --error=audit.out
#SBATCH --time=02:00:00
#SBATCH --ntasks-per-node=1

set -x
python --version
#export PATH=$HOME/parallel-install/bin:$PATH
which parallel
parallel --version
# Log the start time of the job
echo "Job started at: $(date)" >> timing.log

# Execute each step and log the times
for step in 1 2 3 4 5
do
    cat ./steps/step${step}.sh
    start_time=$(date +%s.%N)
    tail ./steps/step${step}.sh | parallel   -j2
    end_time=$(date +%s.%N)
    elapsed_time=$(echo "$end_time - $start_time" | bc)
    elapsed_time=$(printf "%.3f" "$elapsed_time")

    echo "step$step finished in $elapsed_time seconds" >> timing.log
done

# Log the end time of the job
echo "Job ended at: $(date)" >> timing.log
