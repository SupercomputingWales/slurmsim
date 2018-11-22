#!/bin/bash

# This script populates the slurmdb with "users" that submit jobs

export USERS_ACCOUNTS=/home/slurm/slurm_sim_ws/slurm_sim_tools/scw/users_accounts.csv
export QOS=/home/slurm/slurm_sim_ws/slurm_sim_tools/scw/QOS_data.csv 
export SLURM_CONF=/home/slurm/slurm_sim_ws/sim/scw/baseline/etc/slurm.conf
SACCTMGR=/home/slurm/slurm_sim_ws/slurm_opt/bin/sacctmgr

# populating SlurmDB using Slurm sacctmgr utility
# add QOS
# TODO
#$SACCTMGR -i modify QOS set normal Priority=0
#$SACCTMGR -i add QOS Name=supporters Priority=100
#
## add cluster
#$SACCTMGR -i add cluster Name=scw Fairshare=1 QOS=normal,supporters

# add accounts
# find accounts from SACCT_OUTPUT
ACCOUNT_IDX=2
for ACCOUNT in $(cut -d'|' -f$ACCOUNT_IDX $USERS_ACCOUNTS | tail -n +2 | sort | uniq)
do 
    $SACCTMGR -i add account name=$ACCOUNT Fairshare=100
done


# add users
USER_IDX=1

for USER_ACCOUNT in $(cut -d'|' -f$USER_IDX,$ACCOUNT_IDX $USERS_ACCOUNTS | tail -n +2 | sort | uniq)
do
   USER=$(echo $USER_ACCOUNT | cut -d'|' -f1)
   ACCOUNT=$(echo $USER_ACCOUNT | cut -d'|' -f2)
   $SACCTMGR -i add user name=$USER DefaultAccount=$ACCOUNT MaxSubmitJobs=30
done

$SACCTMGR -i modify user set qoslevel="normal,supporters"

unset SLURM_CONF
