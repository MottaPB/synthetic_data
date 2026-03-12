#!/bin/bash
echo "============================================================"
echo "TESTE COMPLETO: CORRELAÇÃO CEP"
echo "============================================================"
echo ""
# Limpar
rm -rf data/synthetic/*
rm -f state/generation_state.json
# 1. Gerar customers (base de CEPs)
echo "1/2 Gerando 1000 customers..."
python src/csv_to_parquet/cli_generate.py --dataset olist_customers --rows 1000
# Extrair CEPs gerados
python << 'PYEOF'
import polars as pl
customers = pl.read_parquet("data/synthetic/olist_customers_synthetic_*.parquet")
ceps = customers['customer_zip_code_prefix'].unique().sort()
print(f"\n📍 {len(ceps)} CEPs únicos gerados em customers:")
print(ceps.head(10))
# Salvar para referência
with open("/tmp/customer_ceps.txt", "w") as f:
    for cep in ceps.to_list():
        f.write(f"{cep}\n")
PYEOF
echo ""
echo "2/2 Gerando 500 geolocations (usando CEPs de customers)..."
python src/csv_to_parquet/cli_generate.py --dataset olist_geolocation --rows 500
# Validar correlação
python << 'PYEOF'
import polars as pl
print("\n" + "=" * 70)
print("VALIDAÇÃO DE CORRELAÇÃO CEP → COORDENADAS")
print("=" * 70)
# Carregar dados
customers = pl.read_parquet("data/synthetic/olist_customers_synthetic_*.parquet")
geo = pl.read_parquet("data/synthetic/olist_geolocation_synthetic_*.parquet")
# CEPs
cust_ceps = set(customers['customer_zip_code_prefix'].to_list())
geo_ceps = set(geo['geolocation_zip_code_prefix'].to_list())
print(f"\n📊 Estatísticas:")
print(f"  CEPs em customers: {len(cust_ceps):,}")
print(f"  CEPs em geolocation: {len(geo_ceps):,}")
# Correlação
overlap = cust_ceps & geo_ceps
coverage = len(overlap) / len(cust_ceps) * 100 if cust_ceps else 0
print(f"\n🔗 Correlação:")
print(f"  CEPs em comum: {len(overlap):,}")
print(f"  Cobertura: {coverage:.1f}%")
if coverage > 90:
    print(f"  ✅ Excelente correlação!")
elif coverage > 50:
    print(f"  ⚠️  Correlação parcial")
else:
    print(f"  ❌ Pouca correlação")
# Verificar consistência geográfica
print(f"\n🗺️  Consistência Geográfica:")
print(f"  Estados únicos: {geo['geolocation_state'].n_unique()}")
print(f"  Cidades únicas: {geo['geolocation_city'].n_unique()}")
# Mostrar exemplos
print(f"\n📍 Exemplos de geolocalização gerada:")
sample = geo.select([
    'geolocation_zip_code_prefix',
    'geolocation_city',
    'geolocation_state',
    'geolocation_lat',
    'geolocation_lng'
]).head(10)
print(sample)
# Validar coordenadas dentro do Brasil
lat_min = geo['geolocation_lat'].min()
lat_max = geo['geolocation_lat'].max()
lng_min = geo['geolocation_lng'].min()
lng_max = geo['geolocation_lng'].max()
print(f"\n🌍 Limites geográficos:")
print(f"  Latitude:  {lat_min:.2f} a {lat_max:.2f}")
print(f"  Longitude: {lng_min:.2f} a {lng_max:.2f}")
# Brasil: lat -33.75 a 5.27, lng -73.99 a -34.79
valid_lat = -34 <= lat_min and lat_max <= 6
valid_lng = -74 <= lng_min and lng_max <= -34
if valid_lat and valid_lng:
    print(f"  ✅ Coordenadas dentro do Brasil")
else:
    print(f"  ⚠️  Coordenadas fora dos limites esperados")
print("\n" + "=" * 70)
print("✅ TESTE CONCLUÍDO!")
print("=" * 70)
PYEOF
