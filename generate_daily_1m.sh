#!/bin/bash
echo "============================================================"
echo "GERAÇÃO INCREMENTAL DIÁRIA: 1M DE LINHAS PARA D+1"
echo "============================================================"
echo ""
# Verificar se já rodou hoje
TODAY=$(date +%Y-%m-%d)
LOCK_FILE="state/daily_generation_${TODAY}.lock"
if [ -f "$LOCK_FILE" ]; then
    echo "⚠️  Geração já executada hoje: $TODAY"
    echo "   Para forçar nova execução, delete: $LOCK_FILE"
    exit 1
fi
# Obter última data gerada
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
    echo "❌ Erro: Estado não encontrado. Execute bootstrap primeiro."
    exit 1
fi
echo "📅 Última data gerada: $LAST_DATE"
echo "📅 Nova data: D+1"
echo ""
# Distribuição para 1M de linhas
ORDERS=250000
ITEMS=500000
PAYMENTS=250000
REVIEWS=200000
echo "📊 Distribuição para 1M de linhas:"
echo "  Orders: $ORDERS"
echo "  Order items: $ITEMS"
echo "  Payments: $PAYMENTS"
echo "  Reviews: $REVIEWS"
echo "  TOTAL: 1,200,000 linhas"
echo ""
read -p "Continuar? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]
then
    exit 1
fi
START_TIME=$(date +%s)
echo ""
echo "🚀 Gerando dados para D+1..."
echo ""
# Gerar orders (força avançar data)
echo "[1/4] Gerando orders ($ORDERS)..."
python src/csv_to_parquet/cli_generate.py --dataset olist_orders --rows $ORDERS --force
# Gerar items
echo ""
echo "[2/4] Gerando order_items ($ITEMS)..."
python src/csv_to_parquet/cli_generate.py --dataset olist_order_items --rows $ITEMS --force
# Gerar payments
echo ""
echo "[3/4] Gerando order_payments ($PAYMENTS)..."
python src/csv_to_parquet/cli_generate.py --dataset olist_order_payments --rows $PAYMENTS --force
# Gerar reviews
echo ""
echo "[4/4] Gerando order_reviews ($REVIEWS)..."
python src/csv_to_parquet/cli_generate.py --dataset olist_order_reviews --rows $REVIEWS --force
END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))
MINUTES=$((ELAPSED / 60))
SECONDS=$((ELAPSED % 60))
# Criar lock file
touch "$LOCK_FILE"
echo ""
echo "============================================================"
echo "✅ GERAÇÃO DIÁRIA CONCLUÍDA!"
echo "============================================================"
echo ""
echo "⏱️  Tempo: ${MINUTES}m ${SECONDS}s"
echo ""
# Estatísticas
python << 'PYEOF'
import json
from pathlib import Path
state_file = Path("state/generation_state.json")
if state_file.exists():
    with open(state_file) as f:
        state = json.load(f)
    
    if "olist_orders" in state.get("datasets", {}):
        info = state["datasets"]["olist_orders"]
        
        print("📊 Status atual:")
        print(f"  Última data gerada: {info.get('last_synthetic_date')}")
        print(f"  Total de gerações: {info.get('generation_count')}")
        print(f"  Última execução: {info.get('last_execution_date')}")
PYEOF
echo ""
echo "💾 Novos dados em: data/synthetic/"
echo ""
