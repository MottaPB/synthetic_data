import polars as pl
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta, date
import random
import uuid
import hashlib
from .base_generator import BaseSyntheticGenerator
from csv_to_parquet.generators.reference_manager import ReferenceDataManager
from csv_to_parquet.generators.cep_regions import get_region_info, validate_cep
from data_utils.log_config import logs_config
logger = logs_config()

class PolarsGenerator(BaseSyntheticGenerator):
    """Generates synthetic data using Polars, with special handling for geolocation datasets based on CEPs (Brazilian ZIP codes)."""
    
    def __init__(
        self, 
        profile: Dict[str, Any], 
        output_dir: Path, 
        target_date: Optional[date] = None,
        reference_manager: Optional[ReferenceDataManager] = None,
        dependencies: Optional[List[Dict]] = None
    ):
        super().__init__(profile, output_dir)
        self.target_date = target_date
        self.reference_manager = reference_manager
        self.dependencies = dependencies or []
    
    def get_engine_name(self) -> str:
        """Returns the name of the engine (polars)"""
        return "polars"
    
    def generate(self, n_rows: int, output_filename: str) -> Path:
        """
        Generates synthetic data using Polars.
        """
        # DEBUG
        logger.info(f"🔍 DEBUG: output_filename = {output_filename}")
        logger.info(f"🔍 DEBUG: dependencies = {len(self.dependencies)} deps")
        logger.info(f"🔍 DEBUG: reference_manager = {self.reference_manager is not None}")
        
        if self.target_date:
            logger.info(
                f"Generating {n_rows:,} synthetic rows for date: {self.target_date}"
            )
        else:
            logger.info(f"Generating {n_rows:,} synthetic rows with Polars")
        
        # DETECT IF IT IS GEOLOCATION BASED ON FILENAME
        is_geolocation = 'geolocation' in output_filename.lower()
        
        logger.info(f"🔍 is_geolocation = {is_geolocation}")
        
        if is_geolocation and self.dependencies and self.reference_manager:
            logger.info("🗺️  Geolocation dataset detected - using CEP-based generation")
            return self._generate_geolocation(n_rows, output_filename)
        else:
            if is_geolocation:
                logger.warning("⚠️  Geolocation detected but missing dependencies or reference_manager")
                logger.warning(f"   dependencies: {bool(self.dependencies)}")
                logger.warning(f"   reference_manager: {bool(self.reference_manager)}")
        
        logger.info("📊 Using normal generation")
        
        # Usual generation for non-geolocation datasets
        data = {}
        base_dates = None
        
        # First pass: generate primary date column if exists (to use as base for correlated columns)
        primary_date_col = None
        for col_name, col_profile in self.profile.get("columns", {}).items():
            if col_profile.get("type") == "datetime":
                if any(keyword in col_name.lower() for keyword in ['purchase', 'created', 'date']):
                    primary_date_col = col_name
                    break
        
        # Generate coluns of primary date first to use as base for correlated columns
        if primary_date_col:
            col_profile = self.profile["columns"][primary_date_col]
            logger.debug(f"Generating primary date column: {primary_date_col}")
            base_dates = self.generate_column_data(primary_date_col, col_profile, n_rows)
            data[primary_date_col] = base_dates
        
        # Second pass: generate other columns, using base_dates for correlation if needed
        for col_name, col_profile in self.profile.get("columns", {}).items():
            if col_name in data:
                continue
            
            logger.debug(f"Generating column: {col_name}")
            data[col_name] = self.generate_column_data(
                col_name, 
                col_profile, 
                n_rows,
                base_dates=base_dates
            )
        
        # Create DataFrame
        df = pl.DataFrame(data)
        
        # Save Parquet
        output_path = self.output_dir / output_filename
        df.write_parquet(output_path, compression="snappy")
        
        file_size_mb = output_path.stat().st_size / (1024 * 1024)
        
        logger.info(
            f"Generated {output_filename}: "
            f"{n_rows:,} rows x {df.shape[1]} columns "
            f"({file_size_mb:.2f} MB)"
        )
        
        return output_path
    
    def _generate_geolocation(self, n_rows: int, output_filename: str) -> Path:
        """
        Generates geolocation data with correlation between CEP (ZIP code) and coordinates (latitude, longitude), as well as city and state information.
        """
        logger.info(f"🗺️  Generating {n_rows:,} geolocation rows with CEP correlation")
        
        # Obtain CEPs from reference manager based on dependencies (e.g., customers dataset)
        customer_ceps = []
        
        if self.dependencies and self.reference_manager:
            for dep in self.dependencies:
                if dep['dataset'] == 'olist_customers':
                    logger.info("📍 Loading CEPs from customers dataset...")
                    
                    try:
                        customer_ceps = self.reference_manager.get_reference_keys(
                            "olist_customers",
                            "customer_zip_code_prefix",
                            self.target_date
                        )
                        
                        logger.info(f"✅ Found {len(customer_ceps):,} unique CEPs from customers")
                    except Exception as e:
                        logger.error(f"❌ Error loading customer CEPs: {e}")
                        import traceback
                        traceback.print_exc()
                    
                    break
        
        # Fallback: use historical CEPs from profile if not found in reference manager
        if not customer_ceps:
            logger.warning("⚠️  No customer CEPs found, using historical CEPs")
            for col_name, col_profile in self.profile.get("columns", {}).items():
                if 'zip' in col_name.lower() or 'cep' in col_name.lower():
                    if col_profile.get("type") in ["categorical", "id"]:
                        customer_ceps = col_profile.get("unique_values", [])
                        if customer_ceps:
                            logger.info(f"Using {len(customer_ceps):,} CEPs from historical data")
                            break
        
        if not customer_ceps:
            raise ValueError("❌ No CEPs available for geolocation generation")
        
        # Sample CEPs (ZIP codes)
        logger.info(f"🎲 Sampling {n_rows} CEPs from {len(customer_ceps):,} available")
        
        sampled_ceps = self.reference_manager.sample_keys(
            customer_ceps,
            n_rows,
            "random"
        )
        
        logger.info(f"✅ Sampled {len(sampled_ceps):,} CEPs")
        
        # Generate correlated geolocation data based on sampled CEPs
        data = {
            'geolocation_zip_code_prefix': [],
            'geolocation_lat': [],
            'geolocation_lng': [],
            'geolocation_city': [],
            'geolocation_state': []
        }
        
        logger.info("🌍 Generating coordinates for each CEP...")
        
        for i, cep in enumerate(sampled_ceps):
            # Validate CEP
            cep_int = validate_cep(cep)
            
            if cep_int is None:
                cep_int = 1000
            
            # Obtain region info based on CEP prefix
            state, city, lat, lng = get_region_info(cep_int)
            
            data['geolocation_zip_code_prefix'].append(cep)
            data['geolocation_lat'].append(lat)
            data['geolocation_lng'].append(lng)
            data['geolocation_city'].append(city)
            data['geolocation_state'].append(state)
        
        logger.info("✅ Coordinates generated for all CEPs")
        
        # Create DataFrame
        df = pl.DataFrame(data)
        
        # Save Parquet
        output_path = self.output_dir / output_filename
        df.write_parquet(output_path, compression="snappy")
        
        file_size_mb = output_path.stat().st_size / (1024 * 1024)
        
        logger.info(
            f"💾 Generated {output_filename}: "
            f"{n_rows:,} rows x {df.shape[1]} columns "
            f"({file_size_mb:.2f} MB)"
        )
        
        # Statistics
        logger.info(f"📊 CEPs únicos: {df['geolocation_zip_code_prefix'].n_unique():,}")
        logger.info(f"📊 Estados: {df['geolocation_state'].n_unique()}")
        logger.info(f"📊 Cidades: {df['geolocation_city'].n_unique()}")
        
        return output_path
    
    def generate_column_data(
        self, 
        col_name: str, 
        col_profile: Dict[str, Any], 
        n_rows: int,
        base_dates: Optional[List[datetime]] = None
    ) -> list:
        """
        Generates data for a column based on its profile, with special handling for geolocation datasets to ensure correlation between CEP and coordinates. 
        For geolocation datasets, it uses the sampled CEPs to generate corresponding latitude, longitude, city, and state information. 
        For other columns, it generates data based on type and profile specifications, optionally using base_dates for correlated datetime generation.
        """
        col_type = col_profile.get("type", "unknown")
        null_rate = col_profile.get("null_rate", 0)
        
        values = []
        
        # Generation of unique IDs with specific patterns
        if col_type == "id":
            id_pattern = col_profile.get("id_pattern", "custom")
            
            for i in range(n_rows):
                if random.random() < null_rate:
                    values.append(None)
                    continue
                
                if id_pattern == "uuid":
                    value = str(uuid.uuid4())
                elif id_pattern == "hash32":
                    unique_str = f"{col_name}_{datetime.now().timestamp()}_{i}_{random.random()}"
                    value = hashlib.md5(unique_str.encode()).hexdigest()
                elif id_pattern == "hash64":
                    unique_str = f"{col_name}_{datetime.now().timestamp()}_{i}_{random.random()}"
                    value = hashlib.sha256(unique_str.encode()).hexdigest()
                else:
                    unique_str = f"{col_name}_{datetime.now().timestamp()}_{i}_{random.random()}"
                    value = hashlib.md5(unique_str.encode()).hexdigest()
                
                values.append(value)
            
            return values
        
        # Normal generation for other types
        for i in range(n_rows):
            if random.random() < null_rate:
                values.append(None)
                continue
            
            if col_type == "numeric":
                mean = col_profile.get("mean", 0)
                std = col_profile.get("std", 1)
                
                if std == 0:
                    std = abs(mean) * 0.1 if mean != 0 else 1
                
                value = random.gauss(mean, std)
                value = max(value, col_profile.get("min", value))
                value = min(value, col_profile.get("max", value))
                
                if "Int" in col_profile.get("dtype", ""):
                    value = int(round(value))
                else:
                    value = round(value, 2)
                
                values.append(value)
            
            elif col_type == "categorical":
                unique_vals = col_profile.get("unique_values", [])
                if unique_vals:
                    values.append(random.choice(unique_vals))
                else:
                    values.append(None)
            
            elif col_type == "text":
                sample_vals = col_profile.get("sample_values", [])
                if sample_vals:
                    values.append(random.choice(sample_vals))
                else:
                    values.append(None)
            
            elif col_type == "datetime":
                if self.target_date is not None:
                    base_datetime = datetime.combine(self.target_date, datetime.min.time())
                    random_seconds = random.randint(0, 86399)
                    value = base_datetime + timedelta(seconds=random_seconds)
                else:
                    min_date_str = col_profile.get("min")
                    max_date_str = col_profile.get("max")
                    
                    if min_date_str and max_date_str:
                        try:
                            min_date = datetime.fromisoformat(str(min_date_str))
                            max_date = datetime.fromisoformat(str(max_date_str))
                            
                            delta = max_date - min_date
                            random_days = random.randint(0, max(delta.days, 0))
                            value = min_date + timedelta(days=random_days)
                        except:
                            value = datetime.now()
                    else:
                        value = datetime.now()
                
                values.append(value)
            
            elif col_type == "boolean":
                true_rate = col_profile.get("true_rate", 0.5)
                values.append(random.random() < true_rate)
            
            else:
                values.append(None)
        
        return values