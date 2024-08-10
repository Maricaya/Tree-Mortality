#!/bin/bash

python3 -m sciunit2 parallel_exec ./bcm_indexes.sh

python3 -m sciunit2 parallel_exec ./topo.sh

