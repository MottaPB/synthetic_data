#!/usr/bin/env python
"""
Validation script for synthetic data generation
"""
import polars as pl
from pathlib import Path
import json
print("=" * 70)
print("SYNTHETIC DATA GENERATION VALIDATION")
print("=" * 70)
# 1. List generated files
print("\n1️⃣  Generated files:")
synthetic_dir = Path("data/synthetic")
files = sorted(synthetic_dir.glob("*.parquet"))
if not files:
    print("  ❌ No files found!")
else:
    for f in files:
        size_mb = f.stat().st_size / (1024 * 1024)
        df = pl.read_parquet(f)
        print(f"  ✅ {f.name}")
        print(f"     {len(df):,} rows x {df.shape[1]} cols ({size_mb:.2f} MB)")
# 2. Generation state
print("\n2️⃣  Generation state:")
state_file = Path("state/generation_state.json")
if state_file.exists():
    with open(state_file) as f:
        state = json.load(f)
    
    for dataset, info in state.get("datasets", {}).items():
        print(f"\n  {dataset}:")
        print(f"    Last synthetic date: {info.get('last_synthetic_date')}")
        print(f"    Last execution: {info.get('last_execution_date')}")
        print(f"    Generations: {info.get('generation_count')}")
else:
    print("  ⚠️  State file not found")
# 3. Temporal progression validation
print("\n3️⃣  Temporal progression validation:")
orders_files = sorted(synthetic_dir.glob("olist_orders_synthetic_*.parquet"))
if not orders_files:
    # Try bootstrap file
    orders_files = list(synthetic_dir.glob("olist_orders_bootstrap.parquet"))
for f in orders_files:
    df = pl.read_parquet(f)
    date_str = f.stem.split('_')[-1]
    
    # Check if date column exists
    if 'order_purchase_timestamp' in df.columns:
        # Convert to datetime if necessary
        col_type = df['order_purchase_timestamp'].dtype
        
        if col_type == pl.Utf8:
            # String: convert to datetime
            dates = df.select(
                pl.col('order_purchase_timestamp').str.to_datetime()
            )['order_purchase_timestamp'].dt.date().unique().sort()
        elif col_type == pl.Datetime:
            # Already datetime
            dates = df['order_purchase_timestamp'].dt.date().unique().sort()
        else:
            dates = None
        
        print(f"\n  {f.name}:")
        print(f"    Expected date (filename): {date_str}")
        
        if dates is not None and len(dates) > 0:
            print(f"    Dates in data: {dates[0]} to {dates[-1]}")
            print(f"    Date range: {(dates[-1] - dates[0]).days} days")
            
            if f.stem.endswith('bootstrap'):
                print(f"    ✅ Bootstrap file (historical range)")
            else:
                expected = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
                status = "✅" if str(dates[0]) == expected else "⚠️"
                print(f"    Status: {status}")
        else:
            print(f"    ⚠️  Could not extract dates")
    else:
        print(f"\n  {f.name}:")
        print(f"    ⚠️  Column 'order_purchase_timestamp' not found")
# 4. Referential integrity validation
print("\n4️⃣  Referential integrity validation:")
# Check orders → order_items
orders_files = list(synthetic_dir.glob("olist_orders*.parquet"))
items_files = list(synthetic_dir.glob("olist_order_items*.parquet"))
if orders_files and items_files:
    print("\n  Checking order_items → orders...")
    
    # Load all orders
    orders_dfs = [pl.read_parquet(f) for f in orders_files]
    all_orders = pl.concat(orders_dfs)
    
    # Load all items
    items_dfs = [pl.read_parquet(f) for f in items_files]
    all_items = pl.concat(items_dfs)
    
    orders_ids = set(all_orders['order_id'].to_list())
    items_ids = set(all_items['order_id'].to_list())
    
    invalid = items_ids - orders_ids
    
    print(f"    Orders: {len(all_orders):,} rows, {len(orders_ids):,} unique IDs")
    print(f"    Items: {len(all_items):,} rows, {len(items_ids):,} unique order_ids")
    
    if invalid:
        print(f"    ❌ {len(invalid):,} order_ids in items DON'T exist in orders")
    else:
        print(f"    ✅ All {len(items_ids):,} order_ids are valid")
    
    # Ratio
    if len(orders_ids) > 0:
        ratio = len(all_items) / len(all_orders)
        print(f"    📊 Ratio: {ratio:.2f} items per order")
# 5. ZIP code correlation (customers ↔ geolocation)
print("\n5️⃣  ZIP code correlation validation:")
customers_files = list(synthetic_dir.glob("olist_customers*.parquet"))
geo_files = list(synthetic_dir.glob("olist_geolocation*.parquet"))
if customers_files and geo_files:
    customers = pl.read_parquet(customers_files[0])
    geo = pl.read_parquet(geo_files[0])
    
    cust_zips = set(customers['customer_zip_code_prefix'].to_list())
    geo_zips = set(geo['geolocation_zip_code_prefix'].to_list())
    
    overlap = cust_zips & geo_zips
    coverage = len(overlap) / len(geo_zips) * 100 if geo_zips else 0
    
    print(f"\n  ZIP codes in customers: {len(cust_zips):,}")
    print(f"  ZIP codes in geolocation: {len(geo_zips):,}")
    print(f"  ZIP codes in common: {len(overlap):,}")
    print(f"  Coverage: {coverage:.1f}%")
    
    if coverage > 90:
        print(f"  ✅ Excellent correlation!")
    elif coverage > 50:
        print(f"  ⚠️  Partial correlation")
    else:
        print(f"  ❌ Poor correlation")
# 6. Unique IDs validation
print("\n6️⃣  Unique IDs validation:")
id_checks = {
    'olist_customers': 'customer_id',
    'olist_products': 'product_id',
    'olist_sellers': 'seller_id',
    'olist_orders': 'order_id',
    'olist_order_reviews': 'review_id'
}
for dataset, id_col in id_checks.items():
    files = list(synthetic_dir.glob(f"{dataset}*.parquet"))
    if files:
        dfs = [pl.read_parquet(f) for f in files]
        df = pl.concat(dfs)
        
        if id_col in df.columns:
            n_unique = df[id_col].n_unique()
            total = len(df)
            duplicates = total - n_unique
            
            status = "✅" if duplicates == 0 else f"⚠️  {duplicates:,} duplicates"
            print(f"  {dataset:30} {status}")
print("\n" + "=" * 70)
print("✅ VALIDATION COMPLETED!")
print("=" * 70)
