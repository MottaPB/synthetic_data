# 📊 Olist Project - Transformando dados em Parquet

Automated pipeline to convert CSV files into Parquet 

## 🎯 Functionalities

![alt text](images/pipeline.png)

- ✅ **Dependencies management with uv**: Using UV to manage packages instead of pip
    - Since the plan is to use this initiall setup on future data pipelines using Airflow, uv is a goo choice because:
        - It separates development dependencies from production dependencies
        - Since I've had problems with jupyter dependencies and Airflow before, this is a good solution, since I don't have to spin a new environment for notebooks.

- ✅ **Bulk file conversion**: CSV → Parquet with optimized compression
- ✅ **Optimized performance**: Using Polars for an optimized data processing
- ✅ **Exploratory analysis**: Basic EDA scripts

## 📁 Project strcuture

```bash
csv_to_parque/
├── src/                        # Source code
│   └── csv_to_parquet/
│       ├── init.py
│       ├── convert_to_parquet.py   # CSV → Parquet
│       └── eda.py                  # Basic EDA
├── data/
│   ├── raw/                    # CSVs originals (input)
│   └── processed/              # Parquet files
├── pyproject.toml              # Configurations and dependencies
└── README.md
```

## :page_with_curl: Data

[Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)