#!/usr/bin/env bash


current_bin_path="`dirname "$0"`"
base_dir=`cd ${current_bin_path}/../;pwd`

#----------------------依赖打印----------------------------------
echo "base_dir is ${base_dir}"
#加载配置文件,只是查看目的，不用于java的classpath
resource_files=""
for file in ${base_dir}/config/*
do
   if [ -z "${resource_files}" ]; then
		resource_files="$file"
	else
		resource_files="$resource_files:$file"
    fi
done

#加载jar包,只是查看目的，不用于java的classpath
jar_files=""
for file in ${base_dir}/lib/*.jar
do
   if [ -n "${jar_files}" ]; then
   	jar_files="$jar_files:$file"
   else
   	jar_files="$file"
   fi
done

#检查程序运行依赖的配置文件
echo "程序运行依赖的配置文件: ${resource_files}"
#检查程序运行依赖的jars
echo "程序运行依赖的jars: ${jar_files}"

#----------------------程序启动----------------------------------
mkdir -p ${base_dir}/logs
class_path=".:${base_dir}/lib/*"
echo "程序运行依赖的类路径： ${class_path}"
main_class="cn.moretv.doraemon.kafka.sink.Kafka2RedisApp"
echo "程序运行的主类： ${main_class}"
echo "程序运行的参数： $@"
log_name=`echo ${main_class}|awk -F '.' '{print $NF}'`
log_dir=/data/logs/kafka-sink
log_file=${log_dir}/${log_name}-`date +"%Y%m%d-%H%M%S"`.log

java_bin=`which java`
nohup ${java_bin} -Xms2G -Xmx4G -classpath ${class_path} ${main_class} $@ > ${log_file} 2>&1 &
#${java_bin} -Xms2G -Xmx4G -cp ${class_path} ${main_class} $@

