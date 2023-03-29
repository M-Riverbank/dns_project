spark-submit \
--class dsy.read.read_hive \
--master yarn \
--deploy-mode cluster \
--driver-memory 3g \
--executor-memory 2g \
--executor-cores 2 \
/soft/data/dns_project.jar