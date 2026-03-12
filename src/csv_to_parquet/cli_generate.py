#!/usr/bin/env python
"""
CLI para geração de dados sintéticos
"""
import argparse
from pathlib import Path
from data_utils.log_config import logs_config
from csv_to_parquet.orchestration.synthetic_pipeline import SyntheticDataPipeline
import json
logger = logs_config()

def main():
    parser = argparse.ArgumentParser(
        description="Generate synthetic data based on historical profile"
    )
    
    parser.add_argument(
        "--dataset",
        type=str,
        help="Dataset name (ex: olist_orders). If not provided, generates all."
    )
    
    parser.add_argument(
        "--rows",
        type=int,
        help="Number of rows to generate (overrides config)"
    )
    
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/datasets_config.yaml"),
        help="Path to configuration file"
    )
    
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data/processed"),
        help="Directory with historical data"
    )
    
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/synthetic"),
        help="Output directory for synthetic data"
    )
    
    parser.add_argument(
        "--date-suffix",
        type=str,
        help="Date suffix for output files (default: today)"
    )

    parser.add_argument(
        "--status",
        action="store_true",
        help="Show generation status for datasets"
    )
    
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Reset state for dataset (use with --dataset)"
    )
    
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force generation even if already generated today"
    )
    
    args = parser.parse_args()
    
    # Criar pipeline
    pipeline = SyntheticDataPipeline(
        config_path=args.config,
        data_dir=args.data_dir,
        output_dir=args.output_dir
    )
    
    # ========== ADICIONAR LÓGICA DE STATUS ==========
    
    # Comando: --status
    if args.status:
        logger.info("Getting generation status...")
        
        if args.dataset:
            # Status de um dataset específico
            status = pipeline.get_dataset_status(args.dataset)
            print("\n" + "=" * 60)
            print(f"STATUS: {args.dataset}")
            print("=" * 60)
            print(json.dumps(status, indent=2, default=str))
        else:
            # Status de todos os datasets
            print("\n" + "=" * 60)
            print("STATUS: ALL DATASETS")
            print("=" * 60)
            
            for ds_name in pipeline.config["datasets"].keys():
                status = pipeline.get_dataset_status(ds_name)
                print(f"\n{ds_name}:")
                print(json.dumps(status, indent=2, default=str))
        
        return
    
    # Comando: --reset
    if args.reset:
        if not args.dataset:
            logger.error("Error: --reset requires --dataset")
            print("❌ Error: --reset requires --dataset")
            return
        
        logger.info(f"Resetting state for {args.dataset}")
        pipeline.state_manager.reset_dataset(args.dataset)
        print(f"✅ State reset for {args.dataset}")
        return
    
    # ===============================================
    
    # Gerar dados
    if args.dataset:
        # Dataset específico
        logger.info(f"Generating dataset: {args.dataset}")
        
        output_path = pipeline.generate_dataset(
            dataset_name=args.dataset,
            n_rows=args.rows,
            force=args.force  # ← Adicionar force
        )
        
        if output_path:
            logger.info(f"✅ Generated: {output_path}")
            print(f"\n✅ Generated: {output_path}")
        else:
            logger.info("⏭️  Generation skipped (already done today)")
            print("\n⏭️  Generation skipped (already done today)")
            print("💡 Use --force to generate anyway")
    else:
        # Todos os datasets
        logger.info("Generating all datasets...")
        
        n_rows_override = {}
        if args.rows:
            # Aplicar mesmo número para todos
            for ds in pipeline.config["datasets"].keys():
                n_rows_override[ds] = args.rows
        
        results = pipeline.generate_all_datasets(
            n_rows_override=n_rows_override,
            force=args.force  # ← Adicionar force
        )
        
        # Mostrar resumo
        print("\n" + "=" * 60)
        print("GENERATION SUMMARY")
        print("=" * 60)
        
        for dataset, result in results.items():
            status = result["status"]
            if status == "success":
                print(f"✅ {dataset}: {result['path']}")
            elif status == "skipped":
                print(f"⏭️  {dataset}: {result['reason']}")
            else:
                print(f"❌ {dataset}: {result['error']}")
        
        print()
    
    logger.info("✅ CLI execution complete!")

if __name__ == "__main__":
    main()