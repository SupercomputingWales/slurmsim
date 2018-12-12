#!/bin/bash

# This script sets up and runs the generation of the job trace files using Rscript
source /install_files/variables.env

# goes to the directory where we want to edit things in (working directory)
cd $SLURM_SIM_TOOLS/scw/

# begins the R script that reads the job data and generates the trace
Rscript parse_slurm_output_and_write_trace.R































