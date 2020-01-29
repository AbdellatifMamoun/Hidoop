#!/bin/bash
killall java &
wait
./make.sh &
cd src 
./make.sh &
java hdfs/NameNodeImpl &

