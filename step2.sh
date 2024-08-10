#!/bin/bash

python -m sciunit2 parallel_exec ./mortality_folds.sh
python -m sciunit2 parallel_exec ./merge_bcm.sh

