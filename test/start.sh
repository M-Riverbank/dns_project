# hdfs与yarn
start-all.sh

#历史服务器
mr-jobhistory-daemon.sh start historyserver

#元数据服务
nohup hive --service metastore  &

#spark
/soft/spark/sbin/start-history-server.sh
