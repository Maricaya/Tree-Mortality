#!/bin/bash

#SBATCH --job-name=train
#SBATCH --output=train.out
#SBATCH --error=train.out
#SBATCH --time=01:00:00
#SBATCH --ntasks-per-node=1

parallel sciunit repeat ::: e8
