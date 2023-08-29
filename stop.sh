#!/bin/bash
ps -a | grep -w server | awk '{print $1}' | xargs kill -9 $1
ps -a | grep -w client | awk '{print $1}' | xargs kill -9 $1
make clean