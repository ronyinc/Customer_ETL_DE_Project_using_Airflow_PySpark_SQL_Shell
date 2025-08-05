#!/bin/bash

#  Step 1: Validate Input
if [ -z "$1" ]; then
  echo " Usage: ./run_customer_etl.sh <YYYY-MM-DD>"
  exit 1
fi

RUN_DATE="$1"
LANDING_PATH="/opt/spark-apps/landing/customer_etl/"
HDFS_INPUT="/customer_etl/input"
HDFS_OUTPUT="/customer_etl/output/loyalty_snapshot_${RUN_DATE}"
FINAL_CSV="/opt/spark-apps/shared_output/customer_etl/loyalty_snapshot_${RUN_DATE}.csv"

echo " Starting Customer ETL for $RUN_DATE..."

#  Step 2: Upload files to HDFS
echo " Uploading input files to HDFS..."
docker exec hdfs-namenode hdfs dfs -rm -r -f ${HDFS_INPUT}
docker exec hdfs-namenode hdfs dfs -mkdir -p ${HDFS_INPUT}
docker exec hdfs-namenode hdfs dfs -put "${LANDING_PATH}/customers.csv" ${HDFS_INPUT}/
docker exec hdfs-namenode hdfs dfs -put "${LANDING_PATH}/products.json" ${HDFS_INPUT}/
docker exec hdfs-namenode hdfs dfs -put "${LANDING_PATH}/orders.csv" ${HDFS_INPUT}/

# Step 3: Run Spark Job
echo " Running Spark job..."
docker exec spark-master spark-submit /opt/spark-apps/customer_etl/scripts/customer_etl_job.py "$RUN_DATE"

# Step 4: Export to CSV (getmerge)
echo " Exporting CSV from HDFS..."

  #mkdir -p "/opt/spark-apps/shared_output/customer_etl/${RUN_DATE}"
  docker exec hdfs-namenode hdfs dfs -getmerge ${HDFS_OUTPUT}/part-* "${FINAL_CSV}"

echo " Done. Final CSV ready at:"
echo "$FINAL_CSV"

