#!/bin/bash

# This script populates the slurmdb with "users" that submit jobs

export USERS_ACCOUNTS=/home/slurm/slurm_sim_ws/slurm_sim_tools/scw/users_accounts.csv
export QOS=/home/slurm/slurm_sim_ws/slurm_sim_tools/scw/QOS_data.csv 
export SLURM_CONF=/home/slurm/slurm_sim_ws/sim/scw/baseline/etc/slurm.conf
SACCTMGR=/home/slurm/slurm_sim_ws/slurm_opt/bin/sacctmgr



$SACCTMGR -i add cluster sunbird

python3 ./sacctmgr_output_to_sacctmgr_commands.py \
    /home/slurm/slurm_sim_ws/slurm_sim_tools/scw/QOS_data.csv | bash || exit 1

# add accounts
# find accounts from SACCT_OUTPUT
ACCOUNT_IDX=2
for ACCOUNT in $(cut -d'|' -f$ACCOUNT_IDX $USERS_ACCOUNTS | tail -n +2 | sort | uniq)
do 
    $SACCTMGR -i add account name=$ACCOUNT Fairshare=100 || exit 1
done


# add users
USER_IDX=1

for USER_ACCOUNT in $(cut -d'|' -f$USER_IDX,$ACCOUNT_IDX $USERS_ACCOUNTS | tail -n +2 | sort | uniq)
do
   USER=$(echo $USER_ACCOUNT | cut -d'|' -f1)
   ACCOUNT=$(echo $USER_ACCOUNT | cut -d'|' -f2)
   $SACCTMGR -i add user name=$USER DefaultAccount=$ACCOUNT MaxSubmitJobs=30 || exit 1
done

$SACCTMGR -i modify user set qoslevel="normal,supporters" || exit

unset SLURM_CONF
