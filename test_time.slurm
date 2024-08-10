#!/bin/bash

#SBATCH --job-name=time
#SBATCH --output=slurm-time.out
#SBATCH --error=slurm-time.out
#SBATCH --time=02:00:00
#SBATCH --ntasks-per-node=1

tail ./step1.sh | parallel -j2
tail ./step2.sh | parallel -j2
