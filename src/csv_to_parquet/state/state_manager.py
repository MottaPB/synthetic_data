import json
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import Dict, Optional
import logging
import json
logger = logging.getLogger(__name__)

class StateManager:
    """Gerencia o estado da geração de dados sintéticos"""
    
    def __init__(self, state_file: Path):
        self.state_file = state_file
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        self.state = self._load_state()
    
    def _load_state(self) -> Dict:
        """Carrega estado do arquivo JSON"""
        if self.state_file.exists():
            with open(self.state_file, 'r') as f:
                state = json.load(f)
                logger.info(f"State loaded from: {self.state_file}")
                return state
        else:
            logger.info("No existing state found, creating new")
            return {"datasets": {}}
    
    def _save_state(self):
        """Salva estado no arquivo JSON"""
        with open(self.state_file, 'w') as f:
            json.dump(self.state, f, indent=2, default=str)
        logger.info(f"State saved to: {self.state_file}")
    
    def get_last_synthetic_date(self, dataset_name: str) -> Optional[date]:
        """
        Obtém a última data sintética gerada para um dataset.
        
        Args:
            dataset_name: Nome do dataset
        
        Returns:
            Última data sintética gerada ou None se primeira vez
        """
        if dataset_name not in self.state["datasets"]:
            return None
        
        last_date_str = self.state["datasets"][dataset_name].get("last_synthetic_date")
        
        if last_date_str:
            return datetime.fromisoformat(last_date_str).date()
        
        return None
    
    def get_last_execution_date(self, dataset_name: str) -> Optional[date]:
        """
        Obtém a última data de EXECUÇÃO (data real) para um dataset.
        
        Args:
            dataset_name: Nome do dataset
        
        Returns:
            Última data de execução ou None
        """
        if dataset_name not in self.state["datasets"]:
            return None
        
        last_exec_str = self.state["datasets"][dataset_name].get("last_execution_date")
        
        if last_exec_str:
            return datetime.fromisoformat(last_exec_str).date()
        
        return None
    
    def should_generate_today(self, dataset_name: str) -> bool:
        """
        Verifica se deve gerar dados hoje (ainda não gerou hoje).
        
        Args:
            dataset_name: Nome do dataset
        
        Returns:
            True se deve gerar, False se já gerou hoje
        """
        today = date.today()
        last_execution = self.get_last_execution_date(dataset_name)
        
        if last_execution is None:
            logger.info(f"{dataset_name}: First time generation")
            return True
        
        if last_execution < today:
            logger.info(
                f"{dataset_name}: Last execution was {last_execution}, "
                f"today is {today}, can generate"
            )
            return True
        
        logger.warning(
            f"{dataset_name}: Already generated today ({today}). "
            f"Skipping to avoid duplicate."
        )
        return False
    
    def get_next_synthetic_date(
        self, 
        dataset_name: str, 
        historical_max_date: date
    ) -> date:
        """
        Calcula a próxima data sintética a ser gerada.
        
        Args:
            dataset_name: Nome do dataset
            historical_max_date: Data máxima dos dados históricos
        
        Returns:
            Próxima data sintética a gerar
        """
        last_synthetic = self.get_last_synthetic_date(dataset_name)
        
        if last_synthetic is None:
            # Primeira vez: avançar 1 dia do histórico
            next_date = historical_max_date + timedelta(days=1)
            logger.info(
                f"{dataset_name}: First generation. "
                f"Historical max: {historical_max_date}, "
                f"Next synthetic date: {next_date}"
            )
        else:
            # Já gerou antes: avançar 1 dia da última data sintética
            next_date = last_synthetic + timedelta(days=1)
            logger.info(
                f"{dataset_name}: Last synthetic date: {last_synthetic}, "
                f"Next synthetic date: {next_date}"
            )
        
        return next_date
    
    def update_generation(
        self, 
        dataset_name: str, 
        synthetic_date: date,
        execution_info: Optional[Dict] = None
    ):
        """
        Atualiza o estado após uma geração bem-sucedida.
        
        Args:
            dataset_name: Nome do dataset
            synthetic_date: Data sintética gerada
            execution_info: Informações adicionais da execução
        """
        today = date.today()
        
        if dataset_name not in self.state["datasets"]:
            self.state["datasets"][dataset_name] = {}
        
        # Atualizar data sintética gerada
        self.state["datasets"][dataset_name]["last_synthetic_date"] = synthetic_date.isoformat()
        
        # Atualizar data de execução (hoje)
        self.state["datasets"][dataset_name]["last_execution_date"] = today.isoformat()
        
        # Timestamp completo
        self.state["datasets"][dataset_name]["last_updated_timestamp"] = datetime.now().isoformat()
        
        # Informações adicionais
        if execution_info:
            self.state["datasets"][dataset_name]["last_execution"] = execution_info
        
        # Incrementar contador
        current_count = self.state["datasets"][dataset_name].get("generation_count", 0)
        self.state["datasets"][dataset_name]["generation_count"] = current_count + 1
        
        self._save_state()
        
        logger.info(
            f"{dataset_name}: Updated state - "
            f"Synthetic date: {synthetic_date}, "
            f"Execution date: {today}"
        )
    
    def reset_dataset(self, dataset_name: str):
        """
        Reseta o estado de um dataset.
        
        Args:
            dataset_name: Nome do dataset
        """
        if dataset_name in self.state["datasets"]:
            del self.state["datasets"][dataset_name]
            self._save_state()
            logger.info(f"{dataset_name}: State reset")
    
    def get_generation_count(self, dataset_name: str) -> int:
        """
        Retorna número de gerações realizadas.
        
        Args:
            dataset_name: Nome do dataset
        
        Returns:
            Número de gerações
        """
        if dataset_name not in self.state["datasets"]:
            return 0
        
        return self.state["datasets"][dataset_name].get("generation_count", 0)
    
    def get_dataset_info(self, dataset_name: str) -> Dict:
        """
        Retorna todas as informações do dataset.
        
        Args:
            dataset_name: Nome do dataset
        
        Returns:
            Dict com informações completas
        """
        if dataset_name not in self.state["datasets"]:
            return {
                "dataset": dataset_name,
                "status": "never_generated"
            }
        
        return {
            "dataset": dataset_name,
            "status": "active",
            **self.state["datasets"][dataset_name]
        }