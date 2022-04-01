#!/usr/bin/env bash

#conda activate nextflow20

# script inputs:

input_dir="/lustre/scratch123/hgi/projects/ukbb_scrna/recover"
# how many months and days to add to mtimes:
shift_add_months="27"
shift_add_days="4"
# only operate (shift mtimes) on files older than ..
# in datetime format %Y-%m-%d'
shift_older_than_cutoff="2022-03-29"

clear && python3 sandman_shift_mtimes.py \
		 $input_dir \
		 $shift_add_months \
		 $shift_add_days \
		 $shift_older_than_cutoff
