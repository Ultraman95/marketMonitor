#!/bin/bash

# 获取当前目录
current_dir=$(pwd)
echo "current_dir= $current_dir"
echo "run stop.sh"
./stop.sh
cd $current_dir/script
sleep 2
echo "clean *.con"
rm -rf ./*.con
export LD_LIBRARY_PATH=$current_dir/lib:$LD_LIBRARY_PATH
echo "run marketMonitor"
nohup python3 -u marketMonitor.py > `date +\%F-\%T`.log 2>&1 &
