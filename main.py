import os
import logging
import pandas as pd

from src.data_processor import DataCleaner, ConsistencyChecker, TOLERANCE
from utils.utils import save_to_excel


# --- CONFIGURAÇÃO DO LOGGING ---
LOG_FILE = 'log.log'
LOG_LEVEL = logging.INFO

logging.basicConfig(
    level=LOG_LEVEL,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, mode='w', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


# --- PATHS ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BANCO_FILE = os.path.join(BASE_DIR, 'data', 'Extrato_Banco.xlsx')
BRITECH_FILE = os.path.join(BASE_DIR, 'data', 'Extrato_Britech.xlsx')

OUTPUT_FILE_TOTAL = 'relatorio_comparacao_completa.xlsx'
OUTPUT_FILE_INCONSISTENT = 'relatorio_inconsistencias.xlsx'


def main():
    logger.info("--- Iniciando Verificação de Inconsistências de PU ---")

    COLUNAS_BANCO = ['Aplicação', 'Qtd.', 'PU Atual', 'Código', 'Vcto.', 'Valor Bruto']
    COLUNAS_BRITECH = ['DATA OPERAÇÃO', 'VALOR BRUTO', 'QUANTIDADE', 'DESCRIÇÃO', 'DATA VENCIMENTO']

    try:
        logger.info("1. Processando dados do Banco...")
        df_banco = DataCleaner(BANCO_FILE, COLUNAS_BANCO).prepare_banco_data()

        logger.info("2. Processando dados da Britech...")
        df_britech = DataCleaner(BRITECH_FILE, COLUNAS_BRITECH).prepare_britech_data()

    except Exception as e:
        logger.critical(f"❌ Erro crítico no processamento: {e}", exc_info=True)
        return

    logger.info(f" -> Dados limpos: Banco ({len(df_banco)}), Britech ({len(df_britech)})")

    checker = ConsistencyChecker(df_banco, df_britech)

    logger.info("3. Iniciando conciliação...")
    df_completo = checker.get_comparison_dataframe()

    logger.info(f"4. Conciliação concluída: {len(df_completo)} ativos")
    save_to_excel(df_completo, OUTPUT_FILE_TOTAL, 'Comparacao_Completa')

    logger.info(f"5. Verificando inconsistências (tolerância {TOLERANCE:.2e})...")
    df_inconsistencias = checker.get_inconsistent_dataframe()

    if not df_inconsistencias.empty:
        save_to_excel(df_inconsistencias, OUTPUT_FILE_INCONSISTENT, 'Inconsistencias')
        logger.info(f"✅ Inconsistências encontradas: {len(df_inconsistencias)}")
    else:
        logger.info("Nenhuma inconsistência encontrada.")

    logger.info("--- Fim do processamento ---")


if __name__ == "__main__":
    main()
