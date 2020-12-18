#!/bin/bash
mkdir server_logs

processes=$1

if [ -z $processes ] || [ $processes -lt 1 ]; then
  echo "please indicate a number of processes of at least one"
  exit 0
fi

i=0
base_p2p_port=5003
base_server_port=6003

membership="127.0.0.1:5000"

read -p "------------- Press enter start. After starting, press enter to kill all servers --------------------"

#i=1
#for i in 
#while [ $i -lt $processes ]; do
#    membership="${membership},localhost:$(($base_p2p_port + $i))"
#    i=$(($i + 1))
#done
membership="127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002"
#for i in {5000..5003}; do 
#	membership="${membership},localhost:$(($base_p2p_port + $i))"
#done

i=0
while [ $i -lt $processes ]; do
  java -DlogFilename=logs/node$(($base_p2p_port + $i)) -cp target/asdProj2.jar Main -conf config.properties address=localhost p2p_port=$(($base_p2p_port + $i)) server_port=$(($base_server_port + $i)) initial_membership=$membership 2>&1 | sed "s/^/[$(($base_p2p_port + $i))] /" | tee server_logs/$(($base_p2p_port + $i)).txt &
  echo "launched process on p2p port $(($base_p2p_port + $i)), server port $(($base_server_port + $i))"
  sleep 1
  i=$(($i + 1))
done

sleep 2
read -p "------------- Press enter to kill servers. --------------------"

kill $(ps aux | grep 'asdProj2.jar' | awk '{print $2}')

echo "All processes done!"
