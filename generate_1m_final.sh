#!/bin/bash
echo "============================================================"
echo "GERAÇÃO COMPLETA COM CORRELAÇÃO CEP - 1.57M LINHAS"
echo "============================================================"
echo ""
echo "📊 Ordem de geração (por nível de dependência):"
echo ""
echo "NÍVEL 0 (Independentes):"
echo "  1. customers      52,000 linhas  ← Gera CEPs"
echo "  2. products       10,000 linhas"
echo "  3. sellers         3,000 linhas"
echo ""
echo "NÍVEL 1 (Dependem de nível 0):"
echo "  4. geolocation     5,000 linhas  ← Usa CEPs de customers"
echo "  5. orders        315,000 linhas  ← Usa customer_id"
echo ""
echo "NÍVEL 2 (Dependem de nível 1):"
echo "  6. order_items   620,000 linhas  ← Usa order_id, product_id, seller_id"
echo "  7. order_payments 315,000 linhas ← Usa order_id"
echo "  8. order_reviews 250,000 linhas  ← Usa order_id"
echo ""
echo "  📊 TOTAL: 1,570,000 linhas"
echo ""
read -p "Continuar? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]
then
    exit 1
fi
START_TIME=$(date +%s)
echo ""
echo "🚀 Iniciando geração..."
echo ""
# NÍVEL 0
echo "============================================================"
echo "NÍVEL 0: Datasets Independentes"
echo "============================================================"
echo ""
echo "[1/8] Gerando customers (52k) - BASE DE CEPs..."
python src/csv_to_parquet/cli_generate.py --dataset olist_customers --rows 52000
echo ""
echo "[2/8] Gerando products (10k)..."
python src/csv_to_parquet/cli_generate.py --dataset olist_products --rows 10000
echo ""
echo "[3/8] Gerando sellers (3k)..."
python src/csv_to_parquet/cli_generate.py --dataset olist_sellers --rows 3000
echo ""
# NÍVEL 1
echo "============================================================"
echo "NÍVEL 1: Datasets com Dependências Simples"
echo "============================================================"
echo ""
echo "[4/8] Gerando geolocation (5k) - CORRELACIONADO COM CUSTOMERS..."
python src/csv_to_parquet/cli_generate.py --dataset olist_geolocation --rows 5000
echo ""
echo "[5/8] Gerando orders (315k) - USANDO CUSTOMERS..."
python src/csv_to_parquet/cli_generate.py --dataset olist_orders --rows 315000
echo ""
# NÍVEL 2
echo "============================================================"
echo "NÍVEL 2: Datasets com Múltiplas Dependências"
echo "============================================================"
echo ""
echo "[6/8] Gerando order_items (620k)..."
python src/csv_to_parquet/cli_generate.py --dataset olist_order_items --rows 620000
echo ""
echo "[7/8] Gerando order_payments (315k)..."
python src/csv_to_parquet/cli_generate.py --dataset olist_order_payments --rows 315000
echo ""
echo "[8/8] Gerando order_reviews (250k)..."
python src/csv_to_parquet/cli_generate.py --dataset olist_order_reviews --rows 250000
echo ""
END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))
MINUTES=$((ELAPSED / 60))
SECONDS=$((ELAPSED % 60))
echo "============================================================"
echo "✅ GERAÇÃO CONCLUÍDA!"
echo "============================================================"
echo ""
echo "⏱️  Tempo total: ${MINUTES}m ${SECONDS}s"
echo ""
# Estatísticas
echo "📊 Estatísticas finais:"
python << 'PYEOF'
import polars as pl
from pathlib import Path
synthetic_dir = Path("data/synthetic")
files = sorted(synthetic_dir.glob("*.parquet"))
total_rows = 0
total_size = 0
by_dataset = {}
for f in files:
    df = pl.read_parquet(f)
    rows = len(df)
    size_mb = f.stat().st_size / (1024 * 1024)
    
    total_rows += rows
    total_size += size_mb
    
    dataset = '_'.join(f.stem.split('_')[:-2])
    if dataset not in by_dataset:
        by_dataset[dataset] = {'rows': 0, 'size': 0}
    
    by_dataset[dataset]['rows'] += rows
    by_dataset[dataset]['size'] += size_mb
print("\nPor dataset:")
for ds, info in sorted(by_dataset.items()):
    print(f"  {ds:30} {info['rows']:>10,} linhas  {info['size']:>8.2f} MB")
print(f"\n{'='*55}")
print(f"  {'TOTAL':30} {total_rows:>10,} linhas  {total_size:>8.2f} MB")
print(f"{'='*55}")
PYEOF
echo ""
echo "🔍 Validando correlação CEP..."
python << 'PYEOF'
import polars as pl
from pathlib import Path
customers = pl.read_parquet("data/synthetic/olist_customers_synthetic_*.parquet")
geo = pl.read_parquet("data/synthetic/olist_geolocation_synthetic_*.parquet")
cust_ceps = set(customers['customer_zip_code_prefix'].to_list())
geo_ceps = set(geo['geolocation_zip_code_prefix'].to_list())
overlap = cust_ceps & geo_ceps
coverage = len(overlap) / len(cust_ceps) * 100
print(f"\n📍 Correlação CEP:")
print(f"  CEPs em customers: {len(cust_ceps):,}")
print(f"  CEPs em geolocation: {len(geo_ceps):,}")
print(f"  Cobertura: {coverage:.1f}%")
if coverage > 90:
    print(f"  ✅ Excelente correlação CEP!")
PYEOF
echo ""
echo "💾 Arquivos salvos em: data/synthetic/"
echo ""
