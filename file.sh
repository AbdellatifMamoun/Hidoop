#!/bin/bash
cat filesample.txt filesample.txt > filesample1.txt
wait
mv filesample1.txt filesample.txt &
wait
