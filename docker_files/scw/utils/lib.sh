

mdirname(){
  START=$1
  END=$2
  echo $START\_$END
}

base_container_name(){
  START=$1
  END=$2
  echo $(mdirname $START $END)_base
}

container_name(){
  START=$1
  END=$2
  echo $(mdirname $START $END)
}

committed_image_name(){
  START=$1
  END=$2
  echo $(mdirname $START $END)
}

sacct_output_filename(){
  START=$1
  END=$2
  echo sacct_output_$START\_$END.csv 
}

cycle(){
FUN=$1
WAIT=$2 # optional
while read START END
do
    DIR=$(mdirname $START $END)
    echo "Cycling on dir $DIR"
    mkdir -p $DIR
    cd $DIR
    $FUN $START $END
    $WAIT
    cd -
done < starts_ends.txt

}

wait_for_ps_to_be_less_than_10(){

   echo 'Waiting a bit anyway...'
   sleep 10
   echo 'Checking...'
   while  [ "$(docker ps | wc -l )" -ge "10" ]
   do
      echo 'Too many containers running. Waiting 5s'
      sleep 5
   done
   echo 'Go!'
}

create_scw_tar(){
  START=$1
  END=$2

  cp ../$(sacct_output_filename $START $END) ./sacct_output.csv
  cp ../hawk.cfg ./
  cp ../QOS_data.csv ./

  tar -cvf scw.tar sacct_output.csv hawk.cfg QOS_data.csv

}


make_base_container(){
  START=$1
  END=$2
  CONTAINER_NAME=$(base_container_name $START $END)
  COMMITTED_IMAGE_NAME=$(committed_image_name $START $END)

  docker create --name $CONTAINER_NAME slurmsim_scw_noexec
  docker cp scw.tar $CONTAINER_NAME:/home/slurm/slurm_sim_ws/slurm_sim_tools/scw
  docker commit $CONTAINER_NAME $COMMITTED_IMAGE_NAME
  docker rm $CONTAINER_NAME

}

run_container(){
  START=$1
  END=$2
  COMMITTED_IMAGE_NAME=$(committed_image_name $START $END)
  CONTAINER_NAME=$(container_name $START $END)

  docker run --name $CONTAINER_NAME $COMMITTED_IMAGE_NAME /install_files/run_all.sh

}

get_data_out(){
  START=$1
  END=$2
  CONTAINER_NAME=$(container_name $START $END)

  docker cp $CONTAINER_NAME:/home/slurm/slurm_sim_ws/sim/scw/baseline/results/jobcomp.log ./

}
