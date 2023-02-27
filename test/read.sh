#!/usr/bin/env bash
#########################################
##
#Desc: 模拟真实的数据生产行为，将原始数据文件的内容逐行读到准备区文件中
##########################################
set -o nounset #必须使用初始化之后的变量
set -o errexit #命令执行过程中如果有报错，直接退出整个脚本
###################################################
#
# _LOG
#
####################################################
# $1 - message
# $2 - log file
_LOG() {
        message="[$(date +'%Y-%m-%d %H:%M:%S') ${0##*/}] $1"
        echo ${message}
        if [[ $# == 2 ]]; then
                echo ${message} >>$(cd `dirname $0`; pwd)/staging.log  ##指定日志文件
        fi
}
#################################################
current_dir=$(cd `dirname $0`; pwd)  ##获取程序所在的绝对路径
batch_num=10    #推送10条数据到kafka之后，sleep一次
current_num=0    ##当前已经推送kafka数量的计数器

while read line
    do
        echo "${line}" >> staging.csv ##将读取的数据写入到准备区文件中
        let current_num+=1  ##推送次数自增
        if [[ ${current_num} -eq ${batch_num} ]]; then
            sleep 1  ##每间隔1秒写一次
            current_num=0 ##计数器归位
        fi
    done < /soft/data/DNS_DATA/dns_data_test.csv  ##指定数据源文件

_LOG "数据文件写入完毕..." ${current_dir}/staging.log

exit 0