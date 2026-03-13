#!/bin/bash
echo "============================================================"
echo "DAILY INCREMENTAL GENERATION: 1M ROWS FOR D+1"
echo "============================================================"
echo ""
# Check if already ran today
TODAY=$(date +%Y-%m-%d)
LOCK_FILE="state/daily_generation_${TODAY}.lock"
if [ -f "$LOCK_FILE" ]; then
    echo "⚠️  Generation already executed today: $TODAY"
    echo "   To force new execution, delete: $LOCK_FILE"
    exit 1
fi
# Get last generated date
LAST_DATE=$(python << 'PYEOF'
import json
from pathlib import Path
state_file = Path("state/generation_state.json")
if state_file.exists():
    with open(state_file) as f:
        state = json.load(f)
    
    if "olist_orders" in state.get("datasets", {}):
        last_date = state["datasets"]["olist_orders"].get("last_synthetic_date")
        print(last_date)
    else:
        print("unknown")
else:
    print("unknown")
PYEOF
)
if [ "$LAST_DATE" == "unknown" ]; then
    echo "❌ Error: State not found. Run bootstrap first."
    exit 1
fi
echo "📅 Last generated date: $LAST_DATE"
echo "📅 New date: D+1"
echo ""
# Distribution for 1M rows
ORDERS=250000
ITEMS=500000
PAYMENTS=250000
REVIEWS=200000
echo "📊 Distribution for 1M rows:"
echo "  Orders: $ORDERS"
echo "  Order items: $ITEMS"
echo "  Payments: $PAYMENTS"
echo "  Reviews: $REVIEWS"
echo "  TOTAL: 1,200,000 rows"
echo ""
read -p "Continue? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]
then
    exit 1
fi
START_TIME=$(date +%s)
echo ""
echo "🚀 Generating data for D+1..."
echo ""
# Generate orders (force date advance)
echo "[1/4] Generating orders ($ORDERS)..."
python src/csv_to_parquet/cli_generate.py --dataset olist_orders --rows $ORDERS --force
# Generate items
echo ""
echo "[2/4] Generating order_items ($ITEMS)..."
python src/csv_to_parquet/cli_generate.py --dataset olist_order_items --rows $ITEMS --force
# Generate payments
echo ""
echo "[3/4] Generating order_payments ($PAYMENTS)..."
python src/csv_to_parquet/cli_generate.py --dataset olist_order_payments --rows $PAYMENTS --force
# Generate reviews
echo ""
echo "[4/4] Generating order_reviews ($REVIEWS)..."
python src/csv_to_parquet/cli_generate.py --dataset olist_order_reviews --rows $REVIEWS --force
END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))
MINUTES=$((ELAPSED / 60))
SECONDS=$((ELAPSED % 60))
# Create lock file
touch "$LOCK_FILE"
echo ""
echo "============================================================"
echo "✅ DAILY GENERATION COMPLETED!"
echo "============================================================"
echo ""
echo "⏱️  Time: ${MINUTES}m ${SECONDS}s"
echo ""
# Statistics
python << 'PYEOF'
import json
from pathlib import Path
state_file = Path("state/generation_state.json")
if state_file.exists():
    with open(state_file) as f:
        state = json.load(f)
    
    if "olist_orders" in state.get("datasets", {}):
        info = state["datasets"]["olist_orders"]
        
        print("📊 Current status:")
        print(f"  Last generated date: {info.get('last_synthetic_date')}")
        print(f"  Total generations: {info.get('generation_count')}")
        print(f"  Last execution: {info.get('last_execution_date')}")
PYEOF
echo ""
echo "💾 New data in: data/synthetic/"
echo ""
