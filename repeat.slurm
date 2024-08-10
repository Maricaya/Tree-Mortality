#!/bin/bash

#SBATCH --job-name=step-repeat
#SBATCH --output=slurm-repeat.out
#SBATCH --error=slurm-repeat.out
#SBATCH --time=01:00:00
#SBATCH --ntasks-per-node=1

#parallel sciunit repeat ::: e8 e9
#parallel sciunit repeat ::: e10 e11
#parallel sciunit repeat ::: e12
#parallel sciunit repeat ::: e13 e14
#parallel sciunit repeat ::: e15

# Log the start time of the entire job
echo "repeat Job started at: $(date)" >> timing.log

# Execute and log each group of parallel tasks
for group in "e8 e9" "e10 e11" "e12" "e13 e14" "e15"
do
    start_time=$(date +%s)
    parallel python3 -m sciunit2 repeat ::: $group
    end_time=$(date +%s)
    elapsed_time=$(($end_time - $start_time))
    echo "Group $group finished in $elapsed_time seconds" >> timing.log
done

# Log the end time of the entire job
echo "repeat Job ended at: $(date)" >> timing.log

