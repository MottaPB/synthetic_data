#!/bin/bash
echo "============================================================"
echo "GERAÇÃO DE 1 MILHÃO DE LINHAS - DADOS SINTÉTICOS"
echo "============================================================"
echo ""
# Datasets e quantidades
# Total: ~1M linhas distribuídas proporcionalmente
echo "📊 Plano de geração:"
echo "  - olist_customers: 50,000 linhas (clientes únicos)"
echo "  - olist_products: 10,000 linhas (produtos únicos)"
echo "  - olist_sellers: 3,000 linhas (vendedores únicos)"
echo "  - olist_orders: 300,000 linhas (pedidos)"
echo "  - olist_order_items: 600,000 linhas (itens de pedidos)"
echo "  Total: ~963,000 linhas"
echo ""
read -p "Continuar? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]
then
    exit 1
fi
echo ""
echo "🚀 Iniciando geração..."
echo ""
# 1. Customers (base de clientes)
echo "1/5 Gerando customers (50k)..."
python src/csv_to_parquet/cli_generate.py \
    --dataset olist_customers \
    --rows 50000
# 2. Products (catálogo de produtos)
echo ""
echo "2/5 Gerando products (10k)..."
python src/csv_to_parquet/cli_generate.py \
    --dataset olist_products \
    --rows 10000
# 3. Sellers (vendedores)
echo ""
echo "3/5 Gerando sellers (3k)..."
python src/csv_to_parquet/cli_generate.py \
    --dataset olist_sellers \
    --rows 3000
# 4. Orders (pedidos)
echo ""
echo "4/5 Gerando orders (300k)..."
python src/csv_to_parquet/cli_generate.py \
    --dataset olist_orders \
    --rows 300000
# 5. Order Items (itens de pedidos)
echo ""
echo "5/5 Gerando order_items (600k)..."
python src/csv_to_parquet/cli_generate.py \
    --dataset olist_order_items \
    --rows 600000
echo ""
echo "============================================================"
echo "✅ GERAÇÃO CONCLUÍDA!"
echo "============================================================"
echo ""
# Mostrar resumo
echo "📊 Resumo:"
ls -lh data/synthetic/
echo ""
# Validar
echo "🔍 Validando..."
python validate_generation.py
