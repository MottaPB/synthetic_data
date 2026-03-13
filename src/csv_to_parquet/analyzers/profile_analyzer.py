import polars as pl
from pathlib import Path
from datetime import datetime, timedelta, date
from typing import Dict, Any, List, Optional
from data_utils.log_config import logs_config
logger = logs_config()

class StatisticalProfiler:
    """Check statistical profile for historical data"""
    
    def __init__(self, data_dir: Path, lookback_days: int = 5):
        self.data_dir = data_dir
        self.lookback_days = lookback_days
    
    def load_historical_data(
        self, 
        file_pattern: str, 
        date_column: Optional[str] = None
    ) -> pl.DataFrame:
        """
        Load historical data from Parquet files matching the pattern.
        
        Args:
            file_pattern: File pattern to match (ex: "orders*.parquet")
            date_column: Date column (not used, kept for compatibility)
        
        Returns:
            DataFrame with historical data
        """
        logger.info(f"Loading historical data: {file_pattern}")
        
        # Buscar arquivos que correspondem ao padrão
        files = list(self.data_dir.glob(file_pattern))
        
        if not files:
            raise FileNotFoundError(f"No files found matching: {file_pattern}")
        
        # Ler todos os arquivos
        dfs = []
        for file in files:
            df = pl.read_parquet(file)
            dfs.append(df)
        
        df_combined = pl.concat(dfs)
        logger.info(f"Loaded {len(df_combined):,} total rows from {len(files)} files")
        
        # Para dados sintéticos, usamos TODOS os dados históricos para análise estatística
        # Não faz sentido filtrar por lookback_days quando os dados são de anos atrás
        
        return df_combined
    
    def analyze_column(self, df: pl.DataFrame, col: str) -> Dict[str, Any]:
        """
        Analyze a column and return its statistical profile.
        """
        # Verificar se DataFrame está vazio
        if len(df) == 0:
            logger.warning(f"DataFrame is empty, cannot analyze column {col}")
            return {
                "name": col,
                "dtype": str(df[col].dtype),
                "type": "unknown",
                "null_rate": 1.0,
                "error": "empty_dataframe"
            }
        
        col_type = df[col].dtype
        null_count = df[col].null_count()
        null_rate = null_count / len(df)
        
        profile = {
            "name": col,
            "dtype": str(col_type),
            "null_rate": null_rate,
            "null_count": null_count,
            "total_count": len(df)
        }
        
        # Análise por tipo
        if col_type in [pl.Int64, pl.Int32, pl.Float64, pl.Float32, pl.Int16, pl.Int8]:
            # Numérico
            non_null = df[col].drop_nulls()
            if len(non_null) > 0:
                profile.update({
                    "type": "numeric",
                    "min": float(non_null.min()),
                    "max": float(non_null.max()),
                    "mean": float(non_null.mean()),
                    "median": float(non_null.median()),
                    "std": float(non_null.std()) if non_null.std() else 0.0,
                    "q25": float(non_null.quantile(0.25)),
                    "q75": float(non_null.quantile(0.75)),
                })
        
        elif col_type == pl.Utf8:
            # Text/Categorical/ID
            unique_values = df[col].drop_nulls().unique()
            n_unique = len(unique_values)
            total_non_null = len(df[col].drop_nulls())
            
            # ========== ADD DETECTION OF ID ==========
            
            # Detect if it's an ID (unique or almost unique column)
            is_id = False
            
            # Criteria to be considered an ID:
            # 1. Name contains 'id' (case insensitive)
            # 2. Values are unique or almost unique (>95% unique)
            if '_id' in col.lower() or col.lower().endswith('id'):
                is_id = True
            elif total_non_null > 0 and (n_unique / total_non_null) > 0.95:
                is_id = True
            
            if is_id:
                # Treat as unique ID
                # Analyze ID pattern (length, character set)
                sample_ids = unique_values.head(100).to_list()
                
                # Check if they look like UUIDs or hashes
                if sample_ids and len(sample_ids[0]) == 32:
                    id_pattern = "hash32"  # Hash MD5 or simillar
                elif sample_ids and len(sample_ids[0]) == 36:
                    id_pattern = "uuid"  # UUID with dashes
                elif sample_ids and len(sample_ids[0]) == 64:
                    id_pattern = "hash64"  # Hash SHA256
                else:
                    id_pattern = "custom"
                
                profile.update({
                    "type": "id",
                    "n_unique": n_unique,
                    "id_pattern": id_pattern,
                    "sample_ids": sample_ids[:10],
                    "avg_length": sum(len(str(x)) for x in sample_ids) / len(sample_ids) if sample_ids else 0
                })
            
            # ============================================
            
            elif n_unique < 100:
                # Categorical
                profile.update({
                    "type": "categorical",
                    "n_unique": n_unique,
                    "unique_values": unique_values.to_list(),
                })
            else:
                # Free text
                try:
                    avg_len = df[col].drop_nulls().str.len_chars().mean()
                except AttributeError:
                    try:
                        avg_len = df[col].drop_nulls().str.n_chars().mean()
                    except AttributeError:
                        non_null_vals = df[col].drop_nulls().to_list()
                        if non_null_vals:
                            avg_len = sum(len(str(x)) for x in non_null_vals) / len(non_null_vals)
                        else:
                            avg_len = 0.0
                
                profile.update({
                    "type": "text",
                    "n_unique": n_unique,
                    "sample_values": unique_values.head(50).to_list(),
                    "avg_length": float(avg_len) if avg_len else 0.0,
                })
        
        elif col_type in [pl.Date, pl.Datetime]:
            # Date/Datetime
            non_null = df[col].drop_nulls()
            if len(non_null) > 0:
                min_date = non_null.min()
                max_date = non_null.max()
                
                # Calculate range in days, handling both date and datetime types
                try:
                    if isinstance(min_date, datetime) and isinstance(max_date, datetime):
                        range_days = (max_date - min_date).days
                    elif hasattr(min_date, 'date') and hasattr(max_date, 'date'):
                        range_days = (max_date.date() - min_date.date()).days
                    else:
                        range_days = 0
                except:
                    range_days = 0
                
                profile.update({
                    "type": "datetime",
                    "min": str(min_date),
                    "max": str(max_date),
                    "range_days": range_days,
                })
        
        elif col_type == pl.Boolean:
            # Boolean
            true_count = (df[col] == True).sum()
            profile.update({
                "type": "boolean",
                "true_rate": true_count / len(df) if len(df) > 0 else 0.0,
                "true_count": true_count,
            })
        
        else:
            # Unkown type
            profile.update({
                "type": "unknown",
                "info": f"Unsupported dtype: {col_type}"
            })
        
        return profile
    
    def analyze_dataset(self, df: pl.DataFrame) -> Dict[str, Any]:
        """
        Analisa dataset completo.
        
        Args:
            df: DataFrame para analisar
        
        Returns:
            Perfil estatístico completo
        """
        logger.info(f"Analyzing dataset: {df.shape[0]:,} rows x {df.shape[1]} columns")
        
        profile = {
            "shape": {
                "rows": df.shape[0],
                "columns": df.shape[1]
            },
            "columns": {}
        }
        
        # Check each column
        for col in df.columns:
            logger.debug(f"Analyzing column: {col}")
            profile["columns"][col] = self.analyze_column(df, col)
        
        logger.info("Dataset analysis complete")
        return profile
    
    def get_profile(
        self, 
        file_pattern: str, 
        date_column: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Obtém perfil estatístico completo dos dados históricos.
        
        Args:
            file_pattern: Padrão do arquivo
            date_column: Coluna de data (opcional)
        
        Returns:
            Perfil estatístico
        """
        df = self.load_historical_data(file_pattern, date_column)
        profile = self.analyze_dataset(df)
        
        # Add metadata about the analysis
        profile["metadata"] = {
            "analyzed_at": datetime.now().isoformat(),
            "lookback_days": self.lookback_days,
            "file_pattern": file_pattern,
            "date_column": date_column,
        }
        
        return profile
    
    def get_max_date(
        self, 
        file_pattern: str, 
        date_column: str
    ) -> Optional[date]:
        """
        Obtain the maximum date from historical data.

        Args:
            file_pattern: File pattern to match (e.g., "orders*.parquet")
            date_column: Date column to analyze
        
        Returns:
            Maximum date found, or None if not found
        """
        if not date_column:
            return None
        
        logger.info(f"Getting max date from {file_pattern}, column: {date_column}")
        
        # Search for files matching the pattern
        files = list(self.data_dir.glob(file_pattern))
        
        if not files:
            raise FileNotFoundError(f"No files found matching: {file_pattern}")
        
        # Read only the date column from all files and find the maximum date
        max_dates = []
        
        for file in files:
            try:
                df = pl.read_parquet(file, columns=[date_column])
                file_max = df[date_column].max()
                
                if file_max:
                    # Convert to date if it's datetime
                    if isinstance(file_max, str):
                        # String: parse to datetime
                        file_max = datetime.fromisoformat(file_max.replace('Z', '+00:00'))
                    
                    if isinstance(file_max, datetime):
                        # Datetime: extract date part
                        file_max = file_max.date()
                    
                    # If it's already a date, keep as is
                    max_dates.append(file_max)
                    
            except Exception as e:
                logger.warning(f"Error reading {file}: {e}")
        
        if not max_dates:
            logger.warning("No valid dates found")
            return None
        
        # Get the overall maximum date
        overall_max = max(max_dates)
        
        logger.info(f"Max date found: {overall_max}")
        
        return overall_max