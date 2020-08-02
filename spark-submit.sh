nohup spark-submit \
--master yarn \
--name "kpi_engine" \
--driver-memory=10G \
--num-executors=25 \
--executor-cores=10 \
--executor-memory=22G \
--conf spark.driver.maxResultSize=0 \
--conf spark.kryoserializer.buffer.max=2047M \
--conf spark.network.timeout=600s \
run_kpi_engine.py > logs/kpi_engine.log &



#nohup spark-submit \
#--master yarn \
#--name "kpi_engine" \
#--deploy-mode client \
#--driver-memory=6G \
#--num-executors=15 \
#--executor-cores=8 \
#--conf spark.dynamicAllocation.enabled=false \
#--executor-memory=20G \
#--conf spark.driver.maxResultSize=0 \
#--conf spark.driver.extraJavaOptions=-XX:+UseG1GC \
#--conf spark.executor.extraJavaOptions=-XX:+UseG1GC \
#--conf spark.sql.autoBroadcastJoinThreshold=-1 \
#--conf spark.yarn.driver.memoryOverhead=2400 \
#--conf spark.yarn.executor.memoryOverhead=2400 \
#--conf spark.yarn.executor.memoryOverhead=600 \
#--conf spark.kryoserializer.buffer.max=2047M \
#--conf spark.network.timeout=600s \
#--conf spark.executor.heartbeatInterval=100s \
#run_kpi_engine.py > logs/kpi_engine.log &
