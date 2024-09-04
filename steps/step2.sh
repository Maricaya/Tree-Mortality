#!/bin/bash

python -m sciunit2 parallel_exec ./scripts/mortality_folds.sh
python -m sciunit2 parallel_exec ./scripts/merge_bcm.sh

