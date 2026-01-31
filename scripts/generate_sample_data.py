"""
=============================================================================
SAMPLE DATA GENERATOR
=============================================================================
Tạo dữ liệu mẫu để test hệ thống khi không có dataset thực từ Kaggle.

Usage:
    python scripts/generate_sample_data.py
    
Output:
    data/raw/sample_events.csv
=============================================================================
"""

import csv
import random
from datetime import datetime, timedelta
import os

# Configuration
OUTPUT_DIR = "data/raw"
OUTPUT_FILE = "sample_events.csv"
NUM_RECORDS = 100000  # Số lượng records cần tạo

# Sample data pools
CATEGORIES = [
    ("electronics.smartphone", "Apple", [599, 799, 999, 1199]),
    ("electronics.smartphone", "Samsung", [399, 599, 799, 999]),
    ("electronics.smartphone", "Xiaomi", [199, 299, 399, 499]),
    ("electronics.laptop", "Apple", [999, 1299, 1799, 2399]),
    ("electronics.laptop", "Dell", [599, 799, 999, 1299]),
    ("electronics.laptop", "Lenovo", [499, 699, 899, 1099]),
    ("electronics.tablet", "Apple", [329, 449, 799, 1099]),
    ("electronics.tablet", "Samsung", [249, 399, 549, 699]),
    ("appliances.kitchen", "Philips", [29, 49, 79, 129]),
    ("appliances.kitchen", "Tefal", [39, 59, 89, 149]),
    ("appliances.environment", "Dyson", [299, 399, 499, 699]),
    ("furniture.living_room", "IKEA", [99, 199, 299, 499]),
    ("furniture.bedroom", "IKEA", [149, 249, 399, 599]),
    ("computers.components", "Corsair", [49, 99, 149, 199]),
    ("computers.components", "Logitech", [29, 59, 99, 149]),
    ("accessories.bag", "Samsonite", [79, 129, 199, 299]),
    ("sport.fitness", "Nike", [49, 79, 129, 199]),
    ("sport.fitness", "Adidas", [39, 69, 99, 149]),
]

EVENT_TYPES = ["view", "cart", "purchase", "remove_from_cart"]
EVENT_WEIGHTS = [70, 20, 8, 2]  # Probability weights

# Generate user IDs (simulating different user groups)
USER_IDS = list(range(100000, 200000))


def generate_event_time(start_date, end_date):
    """Generate random datetime between start and end date."""
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    random_seconds = random.randint(0, 86400)
    return start_date + timedelta(days=random_days, seconds=random_seconds)


def generate_session_id(user_id, event_time):
    """Generate session ID based on user and time."""
    session_date = event_time.strftime("%Y%m%d")
    session_hour = event_time.hour // 4  # 4-hour session windows
    return f"{user_id}_{session_date}_{session_hour}"


def generate_event():
    """Generate a single event record."""
    # Random category and brand
    category_code, brand, prices = random.choice(CATEGORIES)
    category_id = hash(category_code) % 10000000000
    
    # Random product ID (based on category)
    product_id = hash(f"{category_code}_{brand}_{random.randint(1, 100)}") % 10000000000
    product_id = abs(product_id)
    
    # Random price from category price range
    price = random.choice(prices) + random.uniform(-10, 10)
    price = round(max(0.01, price), 2)
    
    # Random event type (weighted)
    event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS)[0]
    
    # Random user
    user_id = random.choice(USER_IDS)
    
    # Random time (last 6 months)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=180)
    event_time = generate_event_time(start_date, end_date)
    
    # Session ID
    user_session = generate_session_id(user_id, event_time)
    
    return {
        "event_time": event_time.strftime("%Y-%m-%d %H:%M:%S UTC"),
        "event_type": event_type,
        "product_id": product_id,
        "category_id": category_id,
        "category_code": category_code,
        "brand": brand,
        "price": price,
        "user_id": user_id,
        "user_session": user_session
    }


def generate_user_journey():
    """
    Generate a realistic user journey (view → cart → purchase).
    This creates more realistic patterns.
    """
    events = []
    
    # Pick a random category/brand/product
    category_code, brand, prices = random.choice(CATEGORIES)
    category_id = abs(hash(category_code) % 10000000000)
    product_id = abs(hash(f"{category_code}_{brand}_{random.randint(1, 100)}") % 10000000000)
    price = random.choice(prices) + random.uniform(-10, 10)
    price = round(max(0.01, price), 2)
    
    # Pick user and time
    user_id = random.choice(USER_IDS)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=180)
    base_time = generate_event_time(start_date, end_date)
    
    user_session = generate_session_id(user_id, base_time)
    
    # Determine journey type
    journey_type = random.choices(
        ["view_only", "view_cart", "view_cart_purchase", "view_purchase"],
        weights=[50, 25, 20, 5]
    )[0]
    
    # Generate events based on journey type
    if journey_type == "view_only":
        # Just views (1-5 views)
        for i in range(random.randint(1, 5)):
            events.append({
                "event_time": (base_time + timedelta(minutes=i*2)).strftime("%Y-%m-%d %H:%M:%S UTC"),
                "event_type": "view",
                "product_id": product_id,
                "category_id": category_id,
                "category_code": category_code,
                "brand": brand,
                "price": price,
                "user_id": user_id,
                "user_session": user_session
            })
    
    elif journey_type == "view_cart":
        # View → Cart (no purchase)
        events.append({
            "event_time": base_time.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "event_type": "view",
            "product_id": product_id,
            "category_id": category_id,
            "category_code": category_code,
            "brand": brand,
            "price": price,
            "user_id": user_id,
            "user_session": user_session
        })
        events.append({
            "event_time": (base_time + timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S UTC"),
            "event_type": "cart",
            "product_id": product_id,
            "category_id": category_id,
            "category_code": category_code,
            "brand": brand,
            "price": price,
            "user_id": user_id,
            "user_session": user_session
        })
    
    elif journey_type == "view_cart_purchase":
        # Full funnel: View → Cart → Purchase
        events.append({
            "event_time": base_time.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "event_type": "view",
            "product_id": product_id,
            "category_id": category_id,
            "category_code": category_code,
            "brand": brand,
            "price": price,
            "user_id": user_id,
            "user_session": user_session
        })
        events.append({
            "event_time": (base_time + timedelta(minutes=3)).strftime("%Y-%m-%d %H:%M:%S UTC"),
            "event_type": "cart",
            "product_id": product_id,
            "category_id": category_id,
            "category_code": category_code,
            "brand": brand,
            "price": price,
            "user_id": user_id,
            "user_session": user_session
        })
        events.append({
            "event_time": (base_time + timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S UTC"),
            "event_type": "purchase",
            "product_id": product_id,
            "category_id": category_id,
            "category_code": category_code,
            "brand": brand,
            "price": price,
            "user_id": user_id,
            "user_session": user_session
        })
    
    else:  # view_purchase (impulse buy)
        events.append({
            "event_time": base_time.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "event_type": "view",
            "product_id": product_id,
            "category_id": category_id,
            "category_code": category_code,
            "brand": brand,
            "price": price,
            "user_id": user_id,
            "user_session": user_session
        })
        events.append({
            "event_time": (base_time + timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S UTC"),
            "event_type": "purchase",
            "product_id": product_id,
            "category_id": category_id,
            "category_code": category_code,
            "brand": brand,
            "price": price,
            "user_id": user_id,
            "user_session": user_session
        })
    
    return events


def main():
    print("="*60)
    print("SAMPLE DATA GENERATOR")
    print("="*60)
    
    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
    
    print(f"\nGenerating {NUM_RECORDS:,} sample events...")
    
    # Generate data
    all_events = []
    
    # Generate user journeys (more realistic patterns)
    num_journeys = NUM_RECORDS // 3  # Average 3 events per journey
    for i in range(num_journeys):
        journey_events = generate_user_journey()
        all_events.extend(journey_events)
        
        if (i + 1) % 10000 == 0:
            print(f"  Generated {i + 1:,} journeys ({len(all_events):,} events)...")
    
    # Add some random events to reach target
    while len(all_events) < NUM_RECORDS:
        all_events.append(generate_event())
    
    # Sort by event time
    all_events.sort(key=lambda x: x["event_time"])
    
    # Limit to exact number
    all_events = all_events[:NUM_RECORDS]
    
    # Write to CSV
    print(f"\nWriting to {output_path}...")
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        fieldnames = ["event_time", "event_type", "product_id", "category_id", 
                      "category_code", "brand", "price", "user_id", "user_session"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_events)
    
    # Summary statistics
    print("\n" + "="*60)
    print("GENERATION COMPLETE!")
    print("="*60)
    print(f"\nOutput file: {output_path}")
    print(f"Total records: {len(all_events):,}")
    
    # Count by event type
    event_counts = {}
    for event in all_events:
        et = event["event_type"]
        event_counts[et] = event_counts.get(et, 0) + 1
    
    print("\nEvent type distribution:")
    for et, count in sorted(event_counts.items(), key=lambda x: -x[1]):
        pct = count * 100.0 / len(all_events)
        print(f"  {et}: {count:,} ({pct:.1f}%)")
    
    # Unique users/products
    unique_users = len(set(e["user_id"] for e in all_events))
    unique_products = len(set(e["product_id"] for e in all_events))
    
    print(f"\nUnique users: {unique_users:,}")
    print(f"Unique products: {unique_products:,}")
    
    print("\n✓ Sample data generated successfully!")
    print("  You can now run: make ingest-bronze")


if __name__ == "__main__":
    main()
