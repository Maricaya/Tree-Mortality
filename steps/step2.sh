#!/bin/bash

sciunit parallel_exec ./scripts/mortality_folds.sh
sciunit parallel_exec ./scripts/merge_bcm.sh

