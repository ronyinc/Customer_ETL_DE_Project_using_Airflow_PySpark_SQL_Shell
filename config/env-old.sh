
#!/bin/bash

# Usage: source env.sh dev OR source env.sh prod
ENV=$1
RUN_DATE=$(date +%F)

echo 'inside env' $ENV
echo "added for test purpose"
if [ "$ENV" == "prod" ]; then
  # Prod HDFS paths
  LANDING_PATH="/opt/spark-apps/landing/customer_etl/"
  HDFS_INPUT="/prod/customer_etl/input"
  HDFS_OUTPUT="/prod/customer_etl/output/loyalty_snapshot_${RUN_DATE}"
  FINAL_CSV="/opt/spark-apps/shared_output/customer_etl/loyalty_snapshot_${RUN_DATE}.csv"
else
  # Dev HDFS paths (default)
  LANDING_PATH="/opt/spark-apps/landing/customer_etl/"
  HDFS_INPUT="/customer_etl/input"
  HDFS_OUTPUT="/customer_etl/output/loyalty_snapshot_${RUN_DATE}"
  FINAL_CSV="/opt/spark-apps/shared_output/customer_etl/loyalty_snapshot_${RUN_DATE}.csv"
fi

