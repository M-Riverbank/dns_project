spark-submit \
--class dsy.model.dnsDailyRecord.importDataToHive \
--master yarn \
--deploy-mode cluster \
--driver-memory 3g \
--executor-memory 2g \
--executor-cores 4 \
/soft/data/dns_project-1.0-SNAPSHOT.jar