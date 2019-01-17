#!/bin/bash
# starts services
/install_files/startup_file.sh 
# unpacks scw.tar and sets up part of the slurm configuration
/install_files/slurm_conf_setup.sh
# runs the actual simulation
/install_files/initial_test.sh
