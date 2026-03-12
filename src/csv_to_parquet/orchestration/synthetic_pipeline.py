from pathlib import Path
from datetime import datetime, date
from typing import Dict, Any, Optional
import yaml
from csv_to_parquet.analyzers.profile_analyzer import StatisticalProfiler
from csv_to_parquet.generators.polars_generator import PolarsGenerator
from csv_to_parquet.state.state_manager import StateManager
from csv_to_parquet.generators.reference_manager import ReferenceDataManager
from data_utils.log_config import logs_config
logger = logs_config()

class SyntheticDataPipeline:
    """Pipeline de geração de dados sintéticos"""
    
    def __init__(
        self, 
        config_path: Path, 
        data_dir: Path, 
        output_dir: Path,
        state_file: Optional[Path] = None
    ):
        self.config_path = config_path
        self.data_dir = data_dir
        self.output_dir = output_dir

        # State manager
        if state_file is None:
            state_file = Path("state/generation_state.json")
        
        self.state_manager = StateManager(state_file)
        
        # Carregar configuração
        with open(config_path) as f:
            self.config = yaml.safe_load(f)
        
        logger.info(f"Pipeline initialized with config: {config_path}")
    
    def generate_dataset(
        self, 
        dataset_name: str, 
        n_rows: Optional[int] = None,
        force: bool = False
    ) -> Optional[Path]:
        """
        Gera dados sintéticos para um dataset com progressão temporal.
        """
        logger.info("=" * 60)
        logger.info(f"Generating synthetic data for: {dataset_name}")
        logger.info("=" * 60)
        
        # Verificar se já gerou hoje
        if not force and not self.state_manager.should_generate_today(dataset_name):
            logger.warning(
                f"⏭️  Skipping {dataset_name}: Already generated today. "
                f"Use --force to override."
            )
            return None
        
        # Obter configuração do dataset
        if dataset_name not in self.config["datasets"]:
            raise ValueError(f"Dataset not found in config: {dataset_name}")
        
        ds_config = self.config["datasets"][dataset_name]
        
        # Número de linhas
        if n_rows is None:
            n_rows = ds_config["target_rows_per_day"]
        
        logger.info(f"Target rows: {n_rows:,}")
        
        # Determinar data alvo
        date_column = ds_config.get("date_column")
        
        if date_column:
            # Dataset tem coluna de data - calcular próxima data
            profiler = StatisticalProfiler(
                self.data_dir, 
                lookback_days=ds_config["lookback_days"]
            )
            
            # Obter data máxima dos dados históricos
            historical_max_date = profiler.get_max_date(
                file_pattern=ds_config["file_pattern"],
                date_column=date_column
            )
            
            if historical_max_date is None:
                raise ValueError(
                    f"Could not determine max date for {dataset_name}. "
                    f"Check if {date_column} exists in data."
                )
            
            logger.info(f"Historical max date: {historical_max_date}")
            
            # Calcular próxima data sintética
            target_date = self.state_manager.get_next_synthetic_date(
                dataset_name, 
                historical_max_date
            )
            
            logger.info(f"Target synthetic date: {target_date}")
        else:
            # Dataset sem coluna de data
            target_date = None
            logger.info("Dataset has no date column, generating without date constraint")
        
        # Analisar perfil estatístico
        profiler = StatisticalProfiler(
            self.data_dir, 
            lookback_days=ds_config["lookback_days"]
        )
        
        profile = profiler.get_profile(
            file_pattern=ds_config["file_pattern"],
            date_column=date_column
        )
        
        logger.info(
            f"Historical data analyzed: "
            f"{profile['shape']['rows']:,} rows from last "
            f"{ds_config['lookback_days']} days"
        )
        
        # Selecionar engine
        engine = self.config["generation"]["engine"]
        spark_threshold = self.config["generation"]["spark_threshold_rows"]
        
        if n_rows > spark_threshold:
            logger.warning(
                f"Rows ({n_rows:,}) exceed Spark threshold ({spark_threshold:,}). "
                f"Consider implementing Spark generator."
            )
        
        # Gerar dados
        if engine == "polars":
            # Criar reference manager
            reference_manager = ReferenceDataManager(
                data_dir=self.data_dir,
                synthetic_dir=self.output_dir
            )
            
            # Obter dependências do config
            dependencies = ds_config.get("dependencies", [])
            
            logger.info(f"Creating generator with {len(dependencies)} dependencies")
            
            # Criar gerador com todas as dependências
            generator = PolarsGenerator(
                profile=profile,
                output_dir=self.output_dir,
                target_date=target_date,
                reference_manager=reference_manager,
                dependencies=dependencies
            )
        else:
            raise NotImplementedError(f"Engine not implemented: {engine}")
        
        # Nome do arquivo com data sintética
        if target_date:
            date_suffix = target_date.strftime("%Y%m%d")
        else:
            date_suffix = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        output_filename = f"{dataset_name}_synthetic_{date_suffix}.parquet"
        
        # ========== GERAR O ARQUIVO ==========
        output_path = generator.generate(n_rows, output_filename)
        # =====================================
        
        # Atualizar estado
        if target_date:
            execution_info = {
                "rows_generated": n_rows,
                "output_file": str(output_path),
                "execution_timestamp": datetime.now().isoformat(),
                "execution_date": date.today().isoformat(),
            }
            
            self.state_manager.update_generation(
                dataset_name, 
                target_date,
                execution_info
            )
            
            gen_count = self.state_manager.get_generation_count(dataset_name)
            logger.info(f"Total generations for {dataset_name}: {gen_count}")
        
        logger.info("=" * 60)
        logger.info(f"✅ Generation complete: {output_path}")
        logger.info("=" * 60)
        
        return output_path
    
    def get_dataset_status(self, dataset_name: str) -> Dict:
        """
        Obtém status atual de um dataset.
        
        Args:
            dataset_name: Nome do dataset
        
        Returns:
            Dict com informações de status
        """
        logger.info(f"Getting status for {dataset_name}")
        
        # Info do state manager
        state_info = self.state_manager.get_dataset_info(dataset_name)
        
        # Verificar se pode gerar hoje
        can_generate = self.state_manager.should_generate_today(dataset_name)
        state_info["can_generate_today"] = can_generate
        
        # Calcular próxima data se possível
        if dataset_name in self.config["datasets"]:
            ds_config = self.config["datasets"][dataset_name]
            date_column = ds_config.get("date_column")
            
            if date_column:
                try:
                    profiler = StatisticalProfiler(
                        self.data_dir, 
                        lookback_days=ds_config["lookback_days"]
                    )
                    
                    historical_max = profiler.get_max_date(
                        ds_config["file_pattern"],
                        date_column
                    )
                    
                    if historical_max:
                        next_synthetic_date = self.state_manager.get_next_synthetic_date(
                            dataset_name,
                            historical_max
                        )
                        
                        state_info["historical_max_date"] = historical_max.isoformat()
                        state_info["next_synthetic_date"] = next_synthetic_date.isoformat()
                except Exception as e:
                    logger.warning(f"Could not calculate next date: {e}")
                    state_info["error"] = str(e)
        
        return state_info
    
    def generate_all_datasets_ordered(
        self,
        n_rows_override: Optional[Dict[str, int]] = None,
        force: bool = False
    ) -> Dict:
        """
        Gera datasets respeitando ordem de dependências (por nível).
        
        Args:
            n_rows_override: Override de linhas por dataset
            force: Forçar geração
        
        Returns:
            Resultados por dataset
        """
        logger.info("Starting ordered generation (respecting dependencies)")
        
        # Agrupar por nível
        datasets_by_level = {}
        for ds_name, ds_config in self.config["datasets"].items():
            level = ds_config.get("level", 0)
            if level not in datasets_by_level:
                datasets_by_level[level] = []
            datasets_by_level[level].append(ds_name)
        
        results = {}
        
        # Gerar por nível (0, 1, 2, ...)
        for level in sorted(datasets_by_level.keys()):
            logger.info("=" * 60)
            logger.info(f"GENERATING LEVEL {level} DATASETS")
            logger.info("=" * 60)
            
            for dataset_name in datasets_by_level[level]:
                logger.info(f"\nDataset: {dataset_name}")
                
                n_rows = None
                if n_rows_override and dataset_name in n_rows_override:
                    n_rows = n_rows_override[dataset_name]
                
                try:
                    output_path = self.generate_dataset(
                        dataset_name,
                        n_rows,
                        force
                    )
                    
                    if output_path:
                        results[dataset_name] = {
                            "status": "success",
                            "level": level,
                            "path": str(output_path)
                        }
                    else:
                        results[dataset_name] = {
                            "status": "skipped",
                            "level": level,
                            "reason": "already_generated_today"
                        }
                except Exception as e:
                    logger.error(f"Failed: {e}", exc_info=True)
                    results[dataset_name] = {
                        "status": "failed",
                        "level": level,
                        "error": str(e)
                    }
            
            logger.info(f"\nLevel {level} complete\n")
        
        return results