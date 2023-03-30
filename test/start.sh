# 启动 yarn 集群 与 hdfs 集群
start-all.sh
# 启动 mr历史服务器
mr-jobhistory-daemon.sh start historyserver
# 启动 spark 集群
#/soft/spark/sbin/start-all.sh
#启动 hive 元数据服务
/soft/hive/bin/hive --service metastore -p 9083 &
# 启动kafka自带的 zookeeper 与 kafka
/soft/kafka/bin/zookeeper-server-start.sh -daemon /soft/kafka/config/zookeeper.properties
/soft/kafka/bin/kafka-server-start.sh -daemon /soft/kafka/config/server.properties

