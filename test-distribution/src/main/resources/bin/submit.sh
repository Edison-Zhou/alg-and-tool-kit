#!/bin/bash

#source ~/.bash_profile


#set -x

Params=($@)
MainClass=${Params[0]}

spark_mainJarName=doraemon-test-1.0.jar
spark_home=/opt/spark2

cd `dirname $0`
pwd=`pwd`

for file in ../conf/*
do
	if [ -n "$resFiles" ]; then
		resFiles="$resFiles,$file"
	else
		resFiles="$file"
    fi
done

resFiles="$resFiles,/opt/hadoop/etc/hadoop/core-site.xml,/opt/hadoop/etc/hadoop/hdfs-site.xml,/opt/spark2/conf/hive-site.xml"

for file in ../lib/*.jar
do
    if [[ "$file" == *${spark_mainJarName} ]]; then
        echo "skip $file"
    else
        if [ -n "$jarFiles" ]; then
            jarFiles="$jarFiles,$file"
        else
            jarFiles="$file"
        fi
    fi
done
ts=`date +%Y%m%d_%H%M%S`
set -x
$spark_home/bin/spark-submit -v \
--name doraemon_test_$ts \
--master yarn \
--executor-memory 4g \
--driver-memory 4g \
--conf spark.shuffle.service.enabled=true \
--conf spark.dynamicAllocation.enabled=true  \
--conf spark.cores.max=50  \
--conf spark.default.parallelism=200 \
--conf spark.dynamicAllocation.maxExecutors=50 \
--files $resFiles \
--jars $jarFiles \
--class "$MainClass" ../lib/${spark_mainJarName}