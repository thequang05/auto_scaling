"""
=============================================================================
DEMO: Schema Evolution with Apache Iceberg
=============================================================================
Demonstrates adding new columns to Iceberg tables without breaking existing data.

Use case: Adding 'payment_method' column to Silver events table
=============================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, rand
from datetime import datetime

def get_spark_session():
    """Create Spark session with Iceberg configuration"""
    spark = SparkSession.builder \
        .appName("Demo-SchemaEvolution") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "rest") \
        .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181") \
        .config("spark.sql.catalog.iceberg.warehouse", "s3://iceberg-warehouse/") \
        .config("spark.sql.catalog.iceberg.s3.endpoint", "http://minio:9000") \
        .config("spark.sql.catalog.iceberg.s3.access-key-id", "minioadmin") \
        .config("spark.sql.catalog.iceberg.s3.secret-access-key", "minioadmin123") \
        .config("spark.sql.catalog.iceberg.s3.path-style-access", "true") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def show_current_schema(spark, table_name: str):
    """Display current schema of a table"""
    print(f"\n{'='*60}")
    print(f"📋 Current Schema: {table_name}")
    print('='*60)
    df = spark.table(table_name)
    df.printSchema()
    print(f"Total columns: {len(df.columns)}")
    return df.columns


def add_column_to_table(spark, table_name: str, column_name: str, column_type: str, comment: str = ""):
    """Add a new column to an Iceberg table using Schema Evolution"""
    print(f"\n{'='*60}")
    print(f"🔄 Schema Evolution: Adding column '{column_name}' to {table_name}")
    print('='*60)
    
    # Check if column already exists
    df = spark.table(table_name)
    if column_name in df.columns:
        print(f"⚠️  Column '{column_name}' already exists. Skipping.")
        return False
    
    # Add new column using ALTER TABLE
    sql = f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}"
    if comment:
        sql += f" COMMENT '{comment}'"
    
    print(f"Executing: {sql}")
    spark.sql(sql)
    print(f"✅ Column '{column_name}' added successfully!")
    return True


def update_column_with_default_values(spark, table_name: str, column_name: str):
    """Update existing rows with default values for the new column"""
    print(f"\n{'='*60}")
    print(f"📝 Updating existing rows with default values for '{column_name}'")
    print('='*60)
    
    # For demo: Set payment_method based on random distribution
    update_sql = f"""
    UPDATE {table_name}
    SET {column_name} = CASE 
        WHEN rand() < 0.4 THEN 'credit_card'
        WHEN rand() < 0.7 THEN 'debit_card'
        WHEN rand() < 0.85 THEN 'paypal'
        WHEN rand() < 0.95 THEN 'bank_transfer'
        ELSE 'cash'
    END
    WHERE {column_name} IS NULL
    """
    
    # Note: For large tables, this could be done in batches
    # For demo, we'll just show the concept
    print("(In production, would update rows. Skipping for demo performance.)")
    print("✅ Concept demonstrated - new column accepts NULL for old data.")


def verify_schema_evolution(spark, table_name: str, original_columns: list):
    """Verify that schema evolution worked correctly"""
    print(f"\n{'='*60}")
    print(f"✅ Verification: Schema Evolution Result")
    print('='*60)
    
    df = spark.table(table_name)
    new_columns = df.columns
    
    print(f"Original columns: {len(original_columns)}")
    print(f"New columns: {len(new_columns)}")
    
    added_columns = [c for c in new_columns if c not in original_columns]
    if added_columns:
        print(f"Added columns: {added_columns}")
    
    # Show sample data
    print(f"\nSample data (showing last 5 columns):")
    last_5_cols = new_columns[-5:]
    df.select(last_5_cols).show(5, truncate=False)
    
    return new_columns


def show_table_history(spark, table_name: str):
    """Show table history to demonstrate schema versions"""
    print(f"\n{'='*60}")
    print(f"📚 Table History (Schema Versions): {table_name}")
    print('='*60)
    
    history_df = spark.sql(f"SELECT * FROM {table_name}.history")
    history_df.show(10, truncate=False)


def main():
    print("\n" + "="*70)
    print("🚀 DEMO: Schema Evolution with Apache Iceberg")
    print("="*70)
    print("This demo shows how to add new columns to Iceberg tables")
    print("without breaking existing data or requiring table recreation.")
    print("="*70)
    
    spark = get_spark_session()
    
    # Target table for schema evolution demo
    # Using silver.events_cleaned as it has the most data
    table_name = "iceberg.silver.events_cleaned"
    
    try:
        # Step 1: Show current schema
        original_columns = show_current_schema(spark, table_name)
        
        # Step 2: Add new column - payment_method
        column_added = add_column_to_table(
            spark, 
            table_name, 
            "payment_method", 
            "STRING",
            "Payment method used for purchase (credit_card, debit_card, paypal, etc.)"
        )
        
        # Step 3: Verify the schema change
        new_columns = verify_schema_evolution(spark, table_name, original_columns)
        
        # Step 4: Show table history (if available)
        try:
            show_table_history(spark, table_name)
        except Exception as e:
            print(f"Note: History view not available: {e}")
        
        # Summary
        print("\n" + "="*70)
        print("📊 SCHEMA EVOLUTION SUMMARY")
        print("="*70)
        print(f"✅ Table: {table_name}")
        print(f"✅ Original columns: {len(original_columns)}")
        print(f"✅ New columns: {len(new_columns)}")
        if column_added:
            print(f"✅ Added: payment_method (STRING)")
            print("\n🎯 Key Benefits of Iceberg Schema Evolution:")
            print("   1. No data rewriting required")
            print("   2. Old data has NULL for new columns (safe)")
            print("   3. No downtime during schema change")
            print("   4. Full history tracking")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        raise e
    finally:
        spark.stop()
    
    print("\n✅ Schema Evolution Demo Complete!")


if __name__ == "__main__":
    main()
