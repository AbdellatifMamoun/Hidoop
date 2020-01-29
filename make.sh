#!/bin/bash
rm */*/*/*.class
cd Serv0/src
javac serv0/HdfsServer.java &
cd ../../Serv1/src
javac serv1/HdfsServer.java &
cd ../../Serv2/src
javac serv2/HdfsServer.java &
cd ../../Serv3/src
javac serv3/HdfsServer.java &
cd ../../Serv4/src
javac serv4/HdfsServer.java &

wait
java serv4/HdfsServer &
cd ../../Serv1/src
java serv1/HdfsServer &
cd ../../Serv2/src
java serv2/HdfsServer &
cd ../../Serv3/src
java serv3/HdfsServer &
cd ../../Serv0/src
java serv0/HdfsServer &

wait

