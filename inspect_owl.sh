#!/bin/bash 

export HADOOP_HOME=/usr/apps/vmas/hadoop-3.2.2 
export SPARK_HOME=/usr/apps/vmas/spark-3.2.0-bin-hadoop3.2 
export PYSPARK_PYTHON=/usr/apps/vmas/anaconda3/bin/python3 
echo "RES: Starting Send_Email" 
echo $(date) 
/usr/apps/vmas/spark-3.2.0-bin-hadoop3.2/bin/spark-submit \
--master spark://njbbvmaspd11.nss.vzwnet.com:7077 \
--conf "spark.sql.session.timeZone=UTC" \
--conf "spark.driver.maxResultSize=16g" \
--conf "spark.dynamicAllocation.enabled=false" \
--num-executors 100 \
--executor-cores 2 \
--total-executor-cores 200 \
--executor-memory 2g \
--driver-memory 16g \
--packages org.apache.spark:spark-avro_2.12:3.2.0,io.streamnative.connectors:pulsar-spark-connector_2.12:3.2.0.2 \
/usr/apps/vmas/scripts/ZS/5g_home_score/inspect_owl.py \
> /usr/apps/vmas/scripts/ZS/5g_home_score/inspect_owl.log 

 
 

 