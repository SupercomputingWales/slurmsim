#!/bin/bash

# This script populates the slurmdb with "users" that submit jobs

source /install_files/variables.sh

export USERS_ACCOUNTS=$SLURM_SIM_TOOLS/scw/users_accounts.csv
export QOS=$SLURM_SIM_TOOLS/scw/QOS_data.csv 
export SLURM_CONF=$SLURM_SIM/scw/baseline/etc/slurm.conf
export SACCTMGR=$SLURM_OPT/bin/sacctmgr



$SACCTMGR -i add cluster sunbird

# setting up the QOSes
python3 /install_files/sacctmgr_output_to_sacctmgr_commands_QOS.py \
    $SLURM_SIM_TOOLS/scw/QOS_data.csv | bash || exit 1



$SACCTMGR -i add account name=slurm_account Fairshare=100
$SACCTMGR -i add user name=slurm DefaultAccount=slurm_account MaxSubmitJobs=1

# THIS IS OBSOLETE SINCE WE HAVE THE DUMP
# add accounts
# find accounts from SACCT_OUTPUT
#ACCOUNT_IDX=2
#for ACCOUNT in $(cut -d'|' -f$ACCOUNT_IDX $USERS_ACCOUNTS | tail -n +2 | sort | uniq)
#do 
#    $SACCTMGR -i add account name=$ACCOUNT Fairshare=100 || exit 1
#done
# THIS WAS OBSOLETE SINCE WE HAVE THE DUMP


# add users, accounts, associations to the database
# using the previously loaded
$SACCTMGR load $CLUSTER_SLURMDB_DUMP
if test "$?" -ne "0"
then 
    echo "Loading of slurm association database dump failed: the command"
    echo "$SACCTMGR load $CLUSTER_SLURMDB_DUMP"
    echo "returned a non zero exit status. Exiting."
fi

unset SLURM_CONF
