#!/bin/bash

# Step 1: Validate Input
if [ -z "$1" ]; then
  echo "Usage: ./customer_etl_job_airflow_version_2.sh <YYYY-MM-DD>"
  exit 1
fi

RUN_DATE="$1"
LANDING_PATH="/opt/spark-apps/landing/customer_etl/"
HDFS_INPUT="/customer_etl/input"
HDFS_OUTPUT="/customer_etl/output/loyalty_snapshot_${RUN_DATE}"
FINAL_CSV="/opt/spark-apps/shared_output/customer_etl/loyalty_snapshot_${RUN_DATE}.csv"

# Step 4: Export result CSV from HDFS to temp path first
TMP_CSV="/tmp/loyalty_snapshot_${RUN_DATE}.csv"

echo " Running Customer ETL for: $RUN_DATE"
echo " Final CSV will be stored at: $FINAL_CSV"

echo " [FORCED MODE] Running as if inside a container (Airflow or Jupyter)"

export PATH=$PATH:/opt/hadoop/bin

# Step 2: Upload input files to HDFS
echo " Uploading input files to HDFS..."

# Just clean individual files to avoid permission issues on the folder
hdfs dfs -mkdir -p ${HDFS_INPUT}

hdfs dfs -rm -f ${HDFS_INPUT}/customers.csv
hdfs dfs -rm -f ${HDFS_INPUT}/products.json
hdfs dfs -rm -f ${HDFS_INPUT}/orders.csv

hdfs dfs -put "${LANDING_PATH}/customers.csv" ${HDFS_INPUT}/
hdfs dfs -put "${LANDING_PATH}/products.json" ${HDFS_INPUT}/
hdfs dfs -put "${LANDING_PATH}/orders.csv" ${HDFS_INPUT}/



# Step 3: Run the Spark job
echo " Running Spark job..."
spark-submit --master spark://spark-master:7077 /opt/spark-apps/customer_etl/scripts/customer_etl_job.py "$RUN_DATE"

# Optional: Clean up any old ETL temp files older than 1 day
# find /tmp -name 'loyalty_snapshot_*.csv' -mtime +1 -exec rm -f {} \;


# Step 4: Export result CSV from HDFS
echo " Exporting merged CSV to temp path..."
hdfs dfs -getmerge "${HDFS_OUTPUT}/part*" "$TMP_CSV" || { echo "getmerge failed"; exit 1; }

# hdfs dfs -getmerge "${HDFS_OUTPUT}/part*" "$FINAL_CSV"

# Move to final location
mv "$TMP_CSV" "$FINAL_CSV"
echo " Moved file to final location: $FINAL_CSV"


echo " Done. Output available at: $FINAL_CSV"