#!/bin/sh

# --app_packages_dir /usr/local/python36/lib/python3.6/site-packages/ \
# /home/zzy/dev_port_concussion-1.0-SNAPSHOT-jar-with-dependencies.jar
# spark_1_app-1.0-SNAPSHOT.jar
# spark_1_app-1.0-SNAPSHOT-jar-with-dependencies.jar

#spark-submit --class "com.ruijie.Batch" \
#spark-submit --class "com.ruijie.KafkaConsumerStreaming" \
#spark-submit --class "com.ruijie.operate.NetSecuSumInfoOperate" >> /home/zzy/log.log \

#--conf "spark.driver.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps" \
#--conf "spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps" \
# --py-files /usr/local/python36/lib/python3.6/site-packages/ \

source /etc/profile
source ~/.bashrc

export PYSPARK_PYTHON="/usr/bin/python3"

spark-submit /home/ion/code.tempPredt/devTempPredt.py \
--name devTempPredt \
--master yarn \
--deploy-mode cluster \
--queue root.default \
--driver-memory 512m \
--num-executors 2 \
--executor-cores 1 \
--executor-memory 512m \
--conf spark.default.parallelism=12 \
--conf spark.yarn.executor.memoryOverhead=512 \
--conf spark.kryoserializer.buffer.max=512 \
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON="/usr/bin/python3"


