"""
Export Gold Layer to CSV for ClickHouse import.
"""

from pyspark.sql import SparkSession

def get_spark_session():
    spark = SparkSession.builder \
        .appName("ExportGoldCSV") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "rest") \
        .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181") \
        .config("spark.sql.catalog.iceberg.warehouse", "s3://iceberg-warehouse/") \
        .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO") \
        .config("spark.sql.catalog.iceberg.s3.endpoint", "http://minio:9000") \
        .config("spark.sql.catalog.iceberg.s3.path-style-access", "true") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.defaultCatalog", "iceberg") \
        .config("spark.jars", "/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.3.jar,/opt/spark/jars/iceberg-aws-1.4.3.jar") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main():
    print("Exporting Gold Layer to CSV...")
    spark = get_spark_session()
    
    tables = [
        ("iceberg.gold.daily_sales_by_category", "/tmp/export/daily_sales"),
        ("iceberg.gold.funnel_analysis", "/tmp/export/funnel_analysis"),
        ("iceberg.gold.customer_rfm", "/tmp/export/customer_rfm"),
        ("iceberg.gold.product_performance", "/tmp/export/product_performance"),
    ]
    
    for table, path in tables:
        try:
            print(f"\nExporting {table}...")
            df = spark.read.table(table)
            count = df.count()
            print(f"  Rows: {count:,}")
            
            # Collect to driver and save locally (small Gold tables)
            pdf = df.toPandas()
            csv_file = f"{path}.csv"
            pdf.to_csv(csv_file, index=False)
            print(f"  ✓ Exported to {csv_file}")
        except Exception as e:
            print(f"  ✗ Error: {str(e)}")
    
    spark.stop()
    print("\nDone! CSV files in /tmp/export/")


if __name__ == "__main__":
    main()
