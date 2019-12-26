#!/usr/bin/env bash

current_bin_path="`dirname "$0"`"
base_dir=`cd ${current_bin_path}/../;pwd`
cd ${base_dir}/lib
rm kafka-sink-1.0.jar
/bin/python /data/tscripts/scripts/ftp.py -s get -f kafka-sink-1.0.jar
md5sum kafka-sink-1.0.jar