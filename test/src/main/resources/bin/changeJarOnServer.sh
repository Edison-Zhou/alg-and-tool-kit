#!/usr/bin/env bash

current_bin_path="`dirname "$0"`"
base_dir=`cd ${current_bin_path}/../;pwd`
cd ${base_dir}/lib

jar_name="doraemon-test-1.0.jar"
rm ${jar_name}
/bin/python /data/tscripts/scripts/ftp.py -s get -f ${jar_name}
md5sum ${jar_name}