#!/usr/bin/env python
import polars as pl
from pathlib import Path
import json
print("=" * 60)
print("VALIDAÇÃO COMPLETA DE DADOS SINTÉTICOS")
print("=" * 60)
# 1. Listar arquivos
print("\n1️⃣  Arquivos gerados:")
synthetic_dir = Path("data/synthetic")
files = sorted(synthetic_dir.glob("*.parquet"))
if not files:
    print("  ❌ Nenhum arquivo encontrado!")
else:
    for f in files:
        size_mb = f.stat().st_size / (1024 * 1024)
        df = pl.read_parquet(f)
        print(f"  ✅ {f.name}")
        print(f"     {len(df):,} linhas x {df.shape[1]} cols ({size_mb:.2f} MB)")
# 2. Estado
print("\n2️⃣  Estado da geração:")
state_file = Path("state/generation_state.json")
if state_file.exists():
    with open(state_file) as f:
        state = json.load(f)
    
    for dataset, info in state.get("datasets", {}).items():
        print(f"\n  {dataset}:")
        print(f"    Última data sintética: {info.get('last_synthetic_date')}")
        print(f"    Última execução: {info.get('last_execution_date')}")
        print(f"    Gerações: {info.get('generation_count')}")
# 3. Validação de datas
print("\n3️⃣  Validação de progressão temporal:")
orders_files = sorted(synthetic_dir.glob("olist_orders_synthetic_*.parquet"))
for f in orders_files:
    df = pl.read_parquet(f)
    date_str = f.stem.split('_')[-1]
    
    # Verificar se a coluna existe
    if 'order_purchase_timestamp' in df.columns:
        # Converter para datetime se necessário
        col_type = df['order_purchase_timestamp'].dtype
        
        if col_type == pl.Utf8:
            # String: converter para datetime
            dates = df.select(
                pl.col('order_purchase_timestamp').str.to_datetime()
            )['order_purchase_timestamp'].dt.date().unique().sort()
        elif col_type == pl.Datetime:
            # Já é datetime
            dates = df['order_purchase_timestamp'].dt.date().unique().sort()
        else:
            dates = None
        
        print(f"\n  {f.name}:")
        print(f"    Data esperada (nome): {date_str}")
        
        if dates is not None and len(dates) > 0:
            print(f"    Datas nos dados: {dates[0]} a {dates[-1]}")
            expected = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
            status = "✅" if str(dates[0]) == expected else "⚠️"
            print(f"    Status: {status}")
        else:
            print(f"    ⚠️  Não foi possível extrair datas")
    else:
        print(f"\n  {f.name}:")
        print(f"    ⚠️  Coluna 'order_purchase_timestamp' não encontrada")
print("\n" + "=" * 60)
print("✅ Validação concluída!")
print("=" * 60)