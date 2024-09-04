#!/bin/bash

python3 -m sciunit2 parallel_exec ./scripts/bcm_indexes.sh

python3 -m sciunit2 parallel_exec ./scripts/topo.sh

