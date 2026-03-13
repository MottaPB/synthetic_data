#!/bin/bash
echo "============================================================"
echo "PRE-BOOTSTRAP VERIFICATION"
echo "============================================================"
echo ""
# Check required files
echo "📁 Checking files..."
files=(
    "config/datasets_config.yaml"
    "src/csv_to_parquet/cli_generate.py"
    "src/csv_to_parquet/generators/polars_generator.py"
    "src/csv_to_parquet/generators/reference_manager.py"
    "src/csv_to_parquet/generators/cep_regions.py"
    "src/csv_to_parquet/state/state_manager.py"
    "bootstrap_10m.sh"
    "generate_daily_1m.sh"
)
all_ok=true
for file in "${files[@]}"; do
    if [ -f "$file" ]; then
        echo "  ✅ $file"
    else
        echo "  ❌ $file - MISSING!"
        all_ok=false
    fi
done
echo ""
echo "📊 Checking historical data..."
if [ -d "data/processed" ] && [ "$(ls -A data/processed/*.parquet 2>/dev/null)" ]; then
    echo "  ✅ data/processed/ ($(ls data/processed/*.parquet | wc -l) files)"
else
    echo "  ❌ data/processed/ - EMPTY!"
    all_ok=false
fi
echo ""
echo "🐍 Checking Python environment..."
python << 'PYEOF'
import sys
try:
    import polars as pl
    print(f"  ✅ polars {pl.__version__}")
except ImportError:
    print("  ❌ polars - NOT INSTALLED!")
    sys.exit(1)
try:
    import pyarrow as pa
    print(f"  ✅ pyarrow {pa.__version__}")
except ImportError:
    print("  ❌ pyarrow - NOT INSTALLED!")
    sys.exit(1)
try:
    import yaml
    print(f"  ✅ pyyaml")
except ImportError:
    print("  ❌ pyyaml - NOT INSTALLED!")
    sys.exit(1)
PYEOF
if [ $? -ne 0 ]; then
    all_ok=false
fi
echo ""
echo "💾 Checking disk space..."
available=$(df -h . | awk 'NR==2 {print $4}')
echo "  Available: $available"
echo "  Required: ~50GB for 10M rows"
echo ""
echo "============================================================"
if [ "$all_ok" = true ]; then
    echo "✅ READY FOR BOOTSTRAP!"
    echo ""
    echo "To execute:"
    echo "  ./bootstrap_10m.sh"
    echo ""
    echo "Estimated time: 2-4 hours"
    echo "Data generated: ~40-50 GB"
else
    echo "❌ FIX THE ISSUES ABOVE BEFORE EXECUTING"
fi
echo "============================================================"
