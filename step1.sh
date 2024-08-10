#!/bin/bash
python -m sciunit2 parallel_exec ./convert_bcm.sh
python -m sciunit2 parallel_exec ./mortality.sh
