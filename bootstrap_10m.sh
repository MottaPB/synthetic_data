#!/bin/bash
echo "============================================================"
echo "BOOTSTRAP: GERAÇÃO INICIAL DE 10M LINHAS"
echo "Distribuídas no range histórico: 2017-09-04 a 2018-10-17"
echo "============================================================"
echo ""
# Limpar estado (começar do zero)
rm -rf data/synthetic/*
rm -f state/generation_state.json
# Range histórico: 409 dias (2017-09-04 a 2018-10-17)
# Vamos gerar ~24.4k linhas por dia para atingir 10M
DAYS=409
ORDERS_PER_DAY=24400
ITEMS_PER_ORDER=2
PAYMENTS_PER_ORDER=1
REVIEWS_RATE=0.8
TOTAL_ORDERS=$((DAYS * ORDERS_PER_DAY))
TOTAL_ITEMS=$((TOTAL_ORDERS * ITEMS_PER_ORDER))
TOTAL_PAYMENTS=$TOTAL_ORDERS
TOTAL_REVIEWS=$((TOTAL_ORDERS * REVIEWS_RATE / 1))
echo "📊 Distribuição:"
echo "  Dias históricos: $DAYS"
echo "  Orders por dia: $ORDERS_PER_DAY"
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
read -p "Continuar com bootstrap? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]
then
    exit 1
fi
START_TIME=$(date +%s)
echo ""
echo "🚀 Iniciando bootstrap..."
echo ""
# NÍVEL 0: Entidades base (sem data)
echo "============================================================"
echo "NÍVEL 0: Entidades Base"
echo "============================================================"
echo ""
echo "[1/8] Gerando customers (500k)..."
python src/csv_to_parquet/cli_generate.py --dataset olist_customers --rows 500000
echo ""
echo "[2/8] Gerando products (50k)..."
python src/csv_to_parquet/cli_generate.py --dataset olist_products --rows 50000
echo ""
echo "[3/8] Gerando sellers (10k)..."
python src/csv_to_parquet/cli_generate.py --dataset olist_sellers --rows 10000
echo ""
echo "[4/8] Gerando geolocation (10k)..."
python src/csv_to_parquet/cli_generate.py --dataset olist_geolocation --rows 10000
# NÍVEL 1 e 2: Dados transacionais (COM data)
# Vamos gerar em lotes para cada dia do range histórico
echo ""
echo "============================================================"
echo "NÍVEIS 1 e 2: Dados Transacionais (409 dias)"
echo "============================================================"
echo ""
echo "[5/8] Gerando orders ($TOTAL_ORDERS total)..."
echo "  Gerando em lotes de $ORDERS_PER_DAY por dia..."
# Gerar orders para cada dia
for i in $(seq 1 $DAYS); do
    if [ $((i % 50)) -eq 0 ]; then
        echo "  Dia $i/$DAYS..."
    fi
    python src/csv_to_parquet/cli_generate.py --dataset olist_orders --rows $ORDERS_PER_DAY --force > /dev/null 2>&1
done
echo "  ✅ $TOTAL_ORDERS orders gerados"
echo ""
echo "[6/8] Gerando order_items ($TOTAL_ITEMS total)..."
# Gerar items para cada dia
for i in $(seq 1 $DAYS); do
    if [ $((i % 50)) -eq 0 ]; then
        echo "  Dia $i/$DAYS..."
    fi
    ITEMS_TODAY=$((ORDERS_PER_DAY * ITEMS_PER_ORDER))
    python src/csv_to_parquet/cli_generate.py --dataset olist_order_items --rows $ITEMS_TODAY --force > /dev/null 2>&1
done
echo "  ✅ $TOTAL_ITEMS items gerados"
echo ""
echo "[7/8] Gerando order_payments ($TOTAL_PAYMENTS total)..."
for i in $(seq 1 $DAYS); do
    if [ $((i % 50)) -eq 0 ]; then
        echo "  Dia $i/$DAYS..."
    fi
    python src/csv_to_parquet/cli_generate.py --dataset olist_order_payments --rows $ORDERS_PER_DAY --force > /dev/null 2>&1
done
echo "  ✅ $TOTAL_PAYMENTS payments gerados"
echo ""
echo "[8/8] Gerando order_reviews ($TOTAL_REVIEWS total)..."
for i in $(seq 1 $DAYS); do
    if [ $((i % 50)) -eq 0 ]; then
        echo "  Dia $i/$DAYS..."
    fi
    REVIEWS_TODAY=$((ORDERS_PER_DAY * REVIEWS_RATE / 1))
    python src/csv_to_parquet/cli_generate.py --dataset olist_order_reviews --rows $REVIEWS_TODAY --force > /dev/null 2>&1
done
echo "  ✅ $TOTAL_REVIEWS reviews gerados"
END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))
HOURS=$((ELAPSED / 3600))
MINUTES=$(((ELAPSED % 3600) / 60))
SECONDS=$((ELAPSED % 60))
echo ""
echo "============================================================"
echo "✅ BOOTSTRAP CONCLUÍDO!"
echo "============================================================"
echo ""
echo "⏱️  Tempo total: ${HOURS}h ${MINUTES}m ${SECONDS}s"
echo ""
# Consolidar arquivos
echo "📦 Consolidando arquivos..."
python << 'PYEOF'
import polars as pl
from pathlib import Path
synthetic_dir = Path("data/synthetic")
# Consolidar cada dataset
datasets = {
    'olist_orders': [],
    'olist_order_items': [],
    'olist_order_payments': [],
    'olist_order_reviews': []
}
print("\n📊 Consolidando datasets...")
for dataset_name in datasets.keys():
    files = sorted(synthetic_dir.glob(f"{dataset_name}_synthetic_*.parquet"))
    
    if files:
        print(f"\n  {dataset_name}: {len(files)} arquivos")
        
        dfs = []
        for f in files:
            df = pl.read_parquet(f)
            dfs.append(df)
        
        # Concatenar
        df_consolidated = pl.concat(dfs)
        
        # Salvar consolidado
        output_file = synthetic_dir / f"{dataset_name}_bootstrap.parquet"
        df_consolidated.write_parquet(output_file, compression="snappy")
        
        print(f"    ✅ {len(df_consolidated):,} linhas → {output_file.name}")
        
        # Remover arquivos individuais
        for f in files:
            f.unlink()
print("\n✅ Consolidação concluída!")
PYEOF
# Estatísticas finais
python << 'PYEOF'
import polars as pl
from pathlib import Path
synthetic_dir = Path("data/synthetic")
files = sorted(synthetic_dir.glob("*.parquet"))
print("\n" + "="*70)
print("ESTATÍSTICAS FINAIS DO BOOTSTRAP")
print("="*70)
total_rows = 0
total_size = 0
for f in files:
    df = pl.read_parquet(f)
    size_mb = f.stat().st_size / (1024 * 1024)
    
    total_rows += len(df)
    total_size += size_mb
    
    print(f"\n  {f.name}")
    print(f"    Linhas: {len(df):,}")
    print(f"    Colunas: {df.shape[1]}")
    print(f"    Tamanho: {size_mb:.2f} MB")
print(f"\n{'='*70}")
print(f"  TOTAL: {total_rows:,} linhas | {total_size:.2f} MB")
print(f"{'='*70}")
PYEOF
echo ""
echo "💾 Dados salvos em: data/synthetic/"
echo ""
