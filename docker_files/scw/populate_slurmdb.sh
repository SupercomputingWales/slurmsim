#!/bin/bash

# This script populates the slurmdb with "users" that submit jobs

export USERS_ACCOUNTS=/home/slurm/slurm_sim_ws/slurm_sim_tools/scw/users_accounts.csv
export QOS=/home/slurm/slurm_sim_ws/slurm_sim_tools/scw/QOS_data.csv 
export SLURM_CONF=/home/slurm/slurm_sim_ws/sim/scw/baseline/etc/slurm.conf
SACCTMGR=/home/slurm/slurm_sim_ws/slurm_opt/bin/sacctmgr



$SACCTMGR -i add cluster sunbird

# setting up the QOSes
python3 ./sacctmgr_output_to_sacctmgr_commands_QOS.py \
    /home/slurm/slurm_sim_ws/slurm_sim_tools/scw/QOS_data.csv | bash || exit 1

# add accounts
# find accounts from SACCT_OUTPUT
ACCOUNT_IDX=2
for ACCOUNT in $(cut -d'|' -f$ACCOUNT_IDX $USERS_ACCOUNTS | tail -n +2 | sort | uniq)
do 
    $SACCTMGR -i add account name=$ACCOUNT Fairshare=100 || exit 1
done


# add users, accounts, associations to the database
# using the previously loaded
sacctmgr load /home/slurm/slurm_sim_ws/slurm_sim_tools/scw/sunbird.cfg 
if test "$?" -ne "0"
then 
    echo "Loading of slurm association database dump failed: the command"
    echo "sacctmgr load /home/slurm/slurm_sim_ws/slurm_sim_tools/scw/sunbird.cfg" 
    echo "returned a non zero exit status. Exiting."
fi

unset SLURM_CONF
