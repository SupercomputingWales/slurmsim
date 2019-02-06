#!/bin/bash

source /install_files/variables.env

echo "This script will untar the file"
echo "$SCW_DATA_PACKAGE"

if [ ! -f "$SCW_DATA_PACKAGE" ]
then
  echo "Error: file $SCW_DATA_PACKAGE, containing job, qos and association data, does not exist. "
  exit 1
else
  cd $(dirname $SCW_DATA_PACKAGE)
  tar -xvf $(basename $SCW_DATA_PACKAGE)
  if [ -f "$NEW_SLURM_CONF" ] 
  then
      echo "Found new slurm.conf. Moving it in the expected location."
      mv $NEW_SLURM_CONF $SLURM_CONF_LOCATION
  fi
  cd -
fi

# creating user.sim file
USERS_SIM=$SLURM_SIM_TOOLS/scw/etc/users.sim
echo "slurm:1000" > $USERS_SIM
grep 'User' $CLUSTER_SLURMDB_DUMP | grep -v root | cut -d\' -f2 | sort | uniq | awk '{print $1":"1000+NR}' >> $USERS_SIM



