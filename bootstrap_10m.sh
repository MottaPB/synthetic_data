#!/bin/bash
echo "============================================================"
echo "BOOTSTRAP: INITIAL GENERATION OF 10M ROWS"
echo "Distributed across historical range: 2017-09-04 to 2018-10-17"
echo "============================================================"
echo ""
# Clear state (start from scratch)
rm -rf data/synthetic/*
rm -f state/generation_state.json
# Historical range: 409 days (2017-09-04 to 2018-10-17)
# Generate ~24.4k rows per day to reach 10M
DAYS=409
ORDERS_PER_DAY=24400
ITEMS_PER_ORDER=2
PAYMENTS_PER_ORDER=1
REVIEWS_RATE=0.8
TOTAL_ORDERS=$((DAYS * ORDERS_PER_DAY))
TOTAL_ITEMS=$((TOTAL_ORDERS * ITEMS_PER_ORDER))
TOTAL_PAYMENTS=$TOTAL_ORDERS
TOTAL_REVIEWS=$((TOTAL_ORDERS * REVIEWS_RATE / 1))
echo "📊 Distribution:"
echo "  Historical days: $DAYS"
echo "  Orders per day: $ORDERS_PER_DAY"
echo ""
echo "  Total orders: $TOTAL_ORDERS (~10M)"
echo "  Total items: $TOTAL_ITEMS (~20M)"
echo "  Total payments: $TOTAL_PAYMENTS (~10M)"
echo "  Total reviews: $TOTAL_REVIEWS (~8M)"
echo ""
echo "  Base entities:"
echo "    Customers: 500,000"
echo "    Products: 50,000"
echo "    Sellers: 10,000"
echo "    Geolocation: 10,000"
echo ""
read -p "Continue with bootstrap? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]
then
    exit 1
fi
START_TIME=$(date +%s)
echo ""
echo "🚀 Starting bootstrap..."
echo ""
# LEVEL 0: Base entities (no date)
echo "============================================================"
echo "LEVEL 0: Base Entities"
echo "============================================================"
echo ""
echo "[1/8] Generating customers (500k)..."
python src/csv_to_parquet/cli_generate.py --dataset olist_customers --rows 500000
echo ""
echo "[2/8] Generating products (50k)..."
python src/csv_to_parquet/cli_generate.py --dataset olist_products --rows 50000
echo ""
echo "[3/8] Generating sellers (10k)..."
python src/csv_to_parquet/cli_generate.py --dataset olist_sellers --rows 10000
echo ""
echo "[4/8] Generating geolocation (10k)..."
python src/csv_to_parquet/cli_generate.py --dataset olist_geolocation --rows 10000
# LEVEL 1 and 2: Transactional data (WITH date)
# Generate in batches for each day of historical range
echo ""
echo "============================================================"
echo "LEVELS 1 & 2: Transactional Data (409 days)"
echo "============================================================"
echo ""
echo "[5/8] Generating orders ($TOTAL_ORDERS total)..."
echo "  Generating in batches of $ORDERS_PER_DAY per day..."
# Generate orders for each day
for i in $(seq 1 $DAYS); do
    if [ $((i % 50)) -eq 0 ]; then
        echo "  Day $i/$DAYS..."
    fi
    python src/csv_to_parquet/cli_generate.py --dataset olist_orders --rows $ORDERS_PER_DAY --force > /dev/null 2>&1
done
echo "  ✅ $TOTAL_ORDERS orders generated"
echo ""
echo "[6/8] Generating order_items ($TOTAL_ITEMS total)..."
# Generate items for each day
for i in $(seq 1 $DAYS); do
    if [ $((i % 50)) -eq 0 ]; then
        echo "  Day $i/$DAYS..."
    fi
    ITEMS_TODAY=$((ORDERS_PER_DAY * ITEMS_PER_ORDER))
    python src/csv_to_parquet/cli_generate.py --dataset olist_order_items --rows $ITEMS_TODAY --force > /dev/null 2>&1
done
echo "  ✅ $TOTAL_ITEMS items generated"
echo ""
echo "[7/8] Generating order_payments ($TOTAL_PAYMENTS total)..."
for i in $(seq 1 $DAYS); do
    if [ $((i % 50)) -eq 0 ]; then
        echo "  Day $i/$DAYS..."
    fi
    python src/csv_to_parquet/cli_generate.py --dataset olist_order_payments --rows $ORDERS_PER_DAY --force > /dev/null 2>&1
done
echo "  ✅ $TOTAL_PAYMENTS payments generated"
echo ""
echo "[8/8] Generating order_reviews ($TOTAL_REVIEWS total)..."
for i in $(seq 1 $DAYS); do
    if [ $((i % 50)) -eq 0 ]; then
        echo "  Day $i/$DAYS..."
    fi
    REVIEWS_TODAY=$((ORDERS_PER_DAY * REVIEWS_RATE / 1))
    python src/csv_to_parquet/cli_generate.py --dataset olist_order_reviews --rows $REVIEWS_TODAY --force > /dev/null 2>&1
done
echo "  ✅ $TOTAL_REVIEWS reviews generated"
END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))
HOURS=$((ELAPSED / 3600))
MINUTES=$(((ELAPSED % 3600) / 60))
SECONDS=$((ELAPSED % 60))
echo ""
echo "============================================================"
echo "✅ BOOTSTRAP COMPLETED!"
echo "============================================================"
echo ""
echo "⏱️  Total time: ${HOURS}h ${MINUTES}m ${SECONDS}s"
echo ""
# Consolidate files
echo "📦 Consolidating files..."
python << 'PYEOF'
import polars as pl
from pathlib import Path
synthetic_dir = Path("data/synthetic")
# Consolidate each dataset
datasets = {
    'olist_orders': [],
    'olist_order_items': [],
    'olist_order_payments': [],
    'olist_order_reviews': []
}
print("\n📊 Consolidating datasets...")
for dataset_name in datasets.keys():
    files = sorted(synthetic_dir.glob(f"{dataset_name}_synthetic_*.parquet"))
    
    if files:
        print(f"\n  {dataset_name}: {len(files)} files")
        
        dfs = []
        for f in files:
            df = pl.read_parquet(f)
            dfs.append(df)
        
        # Concatenate
        df_consolidated = pl.concat(dfs)
        
        # Save consolidated
        output_file = synthetic_dir / f"{dataset_name}_bootstrap.parquet"
        df_consolidated.write_parquet(output_file, compression="snappy")
        
        print(f"    ✅ {len(df_consolidated):,} rows → {output_file.name}")
        
        # Remove individual files
        for f in files:
            f.unlink()
print("\n✅ Consolidation completed!")
PYEOF
# Final statistics
python << 'PYEOF'
import polars as pl
from pathlib import Path
synthetic_dir = Path("data/synthetic")
files = sorted(synthetic_dir.glob("*.parquet"))
print("\n" + "="*70)
print("FINAL BOOTSTRAP STATISTICS")
print("="*70)
total_rows = 0
total_size = 0
for f in files:
    df = pl.read_parquet(f)
    size_mb = f.stat().st_size / (1024 * 1024)
    
    total_rows += len(df)
    total_size += size_mb
    
    print(f"\n  {f.name}")
    print(f"    Rows: {len(df):,}")
    print(f"    Columns: {df.shape[1]}")
    print(f"    Size: {size_mb:.2f} MB")
print(f"\n{'='*70}")
print(f"  TOTAL: {total_rows:,} rows | {total_size:.2f} MB")
print(f"{'='*70}")
PYEOF
echo ""
echo "💾 Data saved in: data/synthetic/"
echo ""
