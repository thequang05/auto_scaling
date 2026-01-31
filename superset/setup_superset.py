"""
=============================================================================
Superset Setup Script
=============================================================================
Script để cấu hình Superset:
- Tạo database connection đến ClickHouse
- Import dashboard templates

Usage:
    python setup_superset.py
=============================================================================
"""

import requests
import json
import time

# Superset configuration
SUPERSET_URL = "http://localhost:8088"
SUPERSET_USERNAME = "admin"
SUPERSET_PASSWORD = "admin"

# ClickHouse connection string
CLICKHOUSE_CONNECTION = "clickhousedb+connect://default:clickhouse123@clickhouse:8123/lakehouse"


def get_access_token():
    """Get JWT access token from Superset."""
    print("Getting access token...")
    
    response = requests.post(
        f"{SUPERSET_URL}/api/v1/security/login",
        json={
            "username": SUPERSET_USERNAME,
            "password": SUPERSET_PASSWORD,
            "provider": "db",
            "refresh": True
        }
    )
    
    if response.status_code == 200:
        token = response.json()["access_token"]
        print("✓ Access token obtained")
        return token
    else:
        raise Exception(f"Failed to get token: {response.text}")


def get_csrf_token(headers):
    """Get CSRF token for write operations."""
    response = requests.get(
        f"{SUPERSET_URL}/api/v1/security/csrf_token/",
        headers=headers
    )
    
    if response.status_code == 200:
        return response.json()["result"]
    return None


def create_database_connection(token):
    """Create ClickHouse database connection in Superset."""
    print("\nCreating ClickHouse database connection...")
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # Get CSRF token
    csrf_token = get_csrf_token(headers)
    if csrf_token:
        headers["X-CSRFToken"] = csrf_token
    
    # Check if connection already exists
    response = requests.get(
        f"{SUPERSET_URL}/api/v1/database/",
        headers=headers
    )
    
    if response.status_code == 200:
        databases = response.json().get("result", [])
        for db in databases:
            if db.get("database_name") == "ClickHouse - Lakehouse":
                print("✓ Database connection already exists")
                return db.get("id")
    
    # Create new connection
    payload = {
        "database_name": "ClickHouse - Lakehouse",
        "sqlalchemy_uri": CLICKHOUSE_CONNECTION,
        "expose_in_sqllab": True,
        "allow_run_async": True,
        "allow_ctas": True,
        "allow_cvas": True,
        "allow_dml": True,
        "extra": json.dumps({
            "engine_params": {
                "connect_args": {
                    "http_timeout": 300,
                    "connect_timeout": 60
                }
            },
            "metadata_params": {},
            "metadata_cache_timeout": {},
            "schemas_allowed_for_file_upload": []
        })
    }
    
    response = requests.post(
        f"{SUPERSET_URL}/api/v1/database/",
        headers=headers,
        json=payload
    )
    
    if response.status_code in [200, 201]:
        db_id = response.json().get("id")
        print(f"✓ Database connection created (ID: {db_id})")
        return db_id
    else:
        print(f"❌ Failed to create database: {response.text}")
        return None


def create_dataset(token, database_id, table_name, schema="lakehouse"):
    """Create a dataset (table) in Superset."""
    print(f"  Creating dataset: {schema}.{table_name}...")
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    csrf_token = get_csrf_token(headers)
    if csrf_token:
        headers["X-CSRFToken"] = csrf_token
    
    payload = {
        "database": database_id,
        "schema": schema,
        "table_name": table_name
    }
    
    response = requests.post(
        f"{SUPERSET_URL}/api/v1/dataset/",
        headers=headers,
        json=payload
    )
    
    if response.status_code in [200, 201]:
        dataset_id = response.json().get("id")
        print(f"    ✓ Dataset created (ID: {dataset_id})")
        return dataset_id
    elif "already exists" in response.text.lower():
        print(f"    ⚠️ Dataset already exists")
        return None
    else:
        print(f"    ❌ Failed: {response.text}")
        return None


def create_all_datasets(token, database_id):
    """Create all required datasets."""
    print("\nCreating datasets...")
    
    tables = [
        "daily_sales",
        "funnel_analysis",
        "customer_rfm",
        "product_performance"
    ]
    
    for table in tables:
        create_dataset(token, database_id, table)


def test_connection(token, database_id):
    """Test the database connection."""
    print("\nTesting database connection...")
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    response = requests.get(
        f"{SUPERSET_URL}/api/v1/database/{database_id}/connection",
        headers=headers
    )
    
    if response.status_code == 200:
        print("✓ Connection test successful")
        return True
    else:
        print(f"❌ Connection test failed: {response.text}")
        return False


def print_dashboard_instructions():
    """Print instructions for creating dashboard manually."""
    print("\n" + "="*60)
    print("📊 SUPERSET DASHBOARD SETUP INSTRUCTIONS")
    print("="*60)
    print("""
1. Access Superset at: http://localhost:8088
   Username: admin
   Password: admin

2. The ClickHouse database connection has been created.

3. To create charts, go to:
   Charts → + Chart → Select "ClickHouse - Lakehouse" database

4. Recommended Charts to Create:

   📈 Chart 1: Daily Revenue Trend (Line Chart)
   - Dataset: daily_sales
   - X-Axis: sale_date
   - Metric: SUM(total_revenue)
   - Time Grain: Day

   🥧 Chart 2: Revenue by Category (Pie Chart)
   - Dataset: daily_sales
   - Dimension: category_level1
   - Metric: SUM(total_revenue)

   📊 Chart 3: Conversion Funnel (Funnel Chart)
   - Dataset: funnel_analysis
   - Create with custom SQL:
     SELECT 'Views' AS stage, SUM(views) AS value FROM funnel_analysis
     UNION ALL
     SELECT 'Carts', SUM(carts) FROM funnel_analysis
     UNION ALL
     SELECT 'Purchases', SUM(purchases) FROM funnel_analysis

   👥 Chart 4: Customer Segments (Bar Chart)
   - Dataset: customer_rfm
   - Dimension: customer_segment
   - Metric: COUNT(*)
   - Sort by metric descending

   📋 Chart 5: Top Products (Table)
   - Dataset: product_performance
   - Columns: product_id, brand, category_level1, total_revenue, purchases
   - Sort by: revenue_rank ASC
   - Row Limit: 10

   🔢 Chart 6: KPI Cards (Big Number)
   - Total Revenue: SUM(total_revenue) from daily_sales
   - Total Orders: SUM(order_count) from daily_sales
   - Conversion Rate: AVG(overall_conversion_rate) from funnel_analysis

5. Create Dashboard:
   - Dashboards → + Dashboard
   - Name: "E-commerce Analytics"
   - Add charts created above
   - Add filters for date range and category

6. Save and share!
""")


def main():
    """Main setup function."""
    print("="*60)
    print("🚀 SUPERSET SETUP FOR DATA LAKEHOUSE")
    print("="*60)
    
    # Wait for Superset to be ready
    print("\nWaiting for Superset to be ready...")
    max_retries = 30
    for i in range(max_retries):
        try:
            response = requests.get(f"{SUPERSET_URL}/health")
            if response.status_code == 200:
                print("✓ Superset is ready")
                break
        except:
            pass
        
        if i < max_retries - 1:
            print(f"  Waiting... ({i+1}/{max_retries})")
            time.sleep(10)
    else:
        print("❌ Superset is not responding. Please check if it's running.")
        return
    
    try:
        # Get access token
        token = get_access_token()
        
        # Create database connection
        db_id = create_database_connection(token)
        
        if db_id:
            # Test connection
            test_connection(token, db_id)
            
            # Create datasets
            create_all_datasets(token, db_id)
        
        # Print manual instructions
        print_dashboard_instructions()
        
        print("\n" + "="*60)
        print("✅ SUPERSET SETUP COMPLETED!")
        print("="*60)
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
