
from pyspark.sql import SparkSession
import sys

def main(run_date):

    spark = SparkSession.builder.appName("CustomerETL2").master("spark://spark-master:7077").getOrCreate()


    df_orders = spark.read.option("header", True).csv("hdfs://hdfs-namenode:9000/customer_etl/input/orders.csv")
    df_products = spark.read.json("hdfs://hdfs-namenode:9000/customer_etl/input/products.json")
    df_customers = spark.read.option("header", True).csv("hdfs://hdfs-namenode:9000/customer_etl/input/customers.csv")

    df_orders.show()


    df_orders.createOrReplaceTempView("orders")
    df_products.createOrReplaceTempView("products")
    df_customers.createOrReplaceTempView("customers")


    spark.sql("""
        CREATE OR REPLACE TEMP VIEW enriched_orders AS
        SELECT
            o.order_id,
            o.customer_id,
            o.product_id,
            o.quantity,
            o.order_date,
            p.category,
            p.unit_price,
            o.quantity * p.unit_price AS total_price
        FROM orders o
        JOIN products p ON o.product_id = p.product_id
    """)

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW customer_metrics AS
        SELECT
            customer_id,
            COUNT(order_id) AS total_orders,
            SUM(total_price) AS total_spent,
            COUNT(DISTINCT order_date) AS days_active,
            COUNT(DISTINCT category) AS categories_bought
        FROM enriched_orders
        GROUP BY customer_id
    """)

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW customer_loyalty AS
        SELECT
            m.customer_id,
            c.customer_name,
            c.city,
            c.state,
            c.signup_date,
            m.total_orders,
            m.total_spent,
            m.days_active,
            m.categories_bought,
            CASE
                WHEN m.total_orders >= 3 AND m.days_active >= 2 AND m.categories_bought >= 2 THEN 'Loyal'
                WHEN m.total_orders >= 2 AND (m.days_active >= 2 OR m.categories_bought >= 2) THEN 'Engaged'
                ELSE 'Casual'
            END AS loyalty_status
        FROM customer_metrics m
        JOIN customers c ON m.customer_id = c.customer_id
    """)

    df_loyalty = spark.sql("SELECT * FROM customer_loyalty")

    df_loyalty.show()


    df_loyalty.write.mode("overwrite").option("header", True).csv(f"hdfs://hdfs-namenode:9000/customer_etl/output/loyalty_snapshot_{run_date}")

    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("You will have to run the script in this format: customer_etl_job.py <YYYY-MM-DD>")
        sys.exit(1)
    main(sys.argv[1])    


