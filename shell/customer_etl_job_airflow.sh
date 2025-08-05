
#!/bin/bash

# Step 1: Validate Input
if [ -z "$1" ]; then
  echo "Usage: ./run_customer_etl.sh <YYYY-MM-DD>"
  exit 1
fi


ENV=${1:-dev}
echo $ENV

#RUN_DATE="$1"
#LANDING_PATH="/opt/spark-apps/landing/customer_etl/"
#HDFS_INPUT="/customer_etl/input"
#HDFS_OUTPUT="/customer_etl/output/loyalty_snapshot_${RUN_DATE}"
#FINAL_CSV="/opt/spark-apps/shared_output/customer_etl/loyalty_snapshot_${RUN_DATE}.csv"

echo " Running Customer ETL for: $RUN_DATE"
echo " Final CSV will be stored at: $FINAL_CSV"

# Step 2: Detect if running inside container
if grep -qE 'docker|containerd' /proc/1/cgroup; then
  echo " Detected: Running INSIDE container (Airflow or Jupyter)"

  export PATH=$PATH:/opt/hadoop/bin
  
  source /opt/spark-apps/customer_etl/config/env.sh "$ENV"

  echo $env
  echo 'HDFS_INPUT' $HDFS_INPUT

  echo " Uploading to HDFS..."
  hdfs dfs -rm -r -f ${HDFS_INPUT}
  hdfs dfs -mkdir -p ${HDFS_INPUT}
  
  hdfs dfs -put "${LANDING_PATH}/customers.csv" ${HDFS_INPUT}/
  hdfs dfs -put "${LANDING_PATH}/products.json" ${HDFS_INPUT}/
  hdfs dfs -put "${LANDING_PATH}/orders.csv" ${HDFS_INPUT}/

  #echo "  Running Spark job..."
  spark-submit --master spark://spark-master:7077 /opt/spark-apps/customer_etl/scripts/customer_etl_job.py "$RUN_DATE"

  echo " Exporting merged CSV to local path..."
  #mkdir -p "/opt/spark-apps/shared_output/customer_etl/${RUN_DATE}"
  echo ${HDFS_OUTPUT} "$FINAL_CSV"
  hdfs dfs -getmerge "${HDFS_OUTPUT}/part*" "$FINAL_CSV"

else
  echo " Detected: Running OUTSIDE container (Ubuntu host)"

  echo " Uploading to HDFS via docker exec..."
  
  source /mnt/c/pyspark_stack/spark-apps/customer_etl/config/env.sh "$ENV"

  echo $env
  echo 'HDFS_INPUT' $HDFS_INPUT
  
  docker exec hdfs-namenode hdfs dfs -rm -r -f ${HDFS_INPUT}
  docker exec hdfs-namenode hdfs dfs -mkdir -p ${HDFS_INPUT}
  #docker exec hdfs-namenode hdfs dfs -put "${LANDING_PATH}/*" ${HDFS_INPUT}/
  docker exec hdfs-namenode hdfs dfs -put "${LANDING_PATH}/customers.csv" ${HDFS_INPUT}/
  docker exec hdfs-namenode hdfs dfs -put "${LANDING_PATH}/products.json" ${HDFS_INPUT}/
  docker exec hdfs-namenode hdfs dfs -put "${LANDING_PATH}/orders.csv" ${HDFS_INPUT}/


  echo " Submitting Spark job via docker exec..."
  docker exec spark-master spark-submit /opt/spark-apps/customer_etl/scripts/customer_etl_job.py "$RUN_DATE"

  echo "Merging HDFS output to host shared folder..."
  #mkdir -p "/opt/spark-apps/shared_output/customer_etl/"
  docker exec hdfs-namenode hdfs dfs -getmerge ${HDFS_OUTPUT}/part-* "$FINAL_CSV"
fi

echo " Done. Output available at: $FINAL_CSV"

