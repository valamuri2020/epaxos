#!/bin/bash
make build
sleep 2
./bin/server -id 0  &
sleep 1
./bin/server -id 1  &
sleep 1
./bin/server -id 2  &

sleep 5
./bin/client -id 0