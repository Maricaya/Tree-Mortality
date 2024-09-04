#!/bin/bash
python3 -m sciunit2 parallel_exec ./scripts/convert_bcm.sh
python3 -m sciunit2 parallel_exec ./scripts/mortality.sh
