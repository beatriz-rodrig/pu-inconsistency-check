# main.py

import os
import pandas as pd
import logging
from src.data_processor import DataCleaner, ConsistencyChecker, TOLERANCE
from typing import List

# --- CONFIGURAÇÃO INICIAL DO LOGGING ---
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

# Define caminhos de arquivo baseados na estrutura do repositório
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BANCO_FILE = os.path.join(BASE_DIR, 'data', 'Extrato_Banco.xlsx')
BRITECH_FILE = os.path.join(BASE_DIR, 'data', 'Extrato_Britech.xlsx')

# Nomes dos arquivos de saída (Relatórios em Excel)
OUTPUT_FILE_TOTAL = 'relatorio_comparacao_completa.xlsx' 
OUTPUT_FILE_INCONSISTENT = 'relatorio_inconsistencias.xlsx' 


def save_to_excel(df: pd.DataFrame, filename: str, sheet_name: str):
    """ Salva o DataFrame em um arquivo Excel com formatação personalizada. """
    try:
        writer = pd.ExcelWriter(filename, engine='xlsxwriter', datetime_format='yyyy-mm-dd')
        
        df.to_excel(writer, sheet_name=sheet_name, index=False)
        
        workbook = writer.book
        worksheet = writer.sheets[sheet_name]
        
        # 1. Definição de Formatos
        currency_fmt = workbook.add_format({'num_format': 'R$ #,##0.00'}) 
        percentage_fmt = workbook.add_format({'num_format': '0.00%'}) 
        pu_fmt = workbook.add_format({'num_format': '0.0000000000'}) 
        date_fmt = workbook.add_format({'num_format': 'dd-mm-yyyy'}) 
        
        # 2. Mapeamento e Aplicação de Formatos
        COLUNA_FORMATO = {
            'VALOR_DIF_REAL': currency_fmt,
            'VALOR_BRUTO_BANCO': currency_fmt, 
            'VALOR_BRUTO_BRITECH': currency_fmt,
            'PU_DIFF_PERC': percentage_fmt,
            'PU_DIFF_VALOR': pu_fmt,
            'PU_BANCO': pu_fmt,
            'PU_BRITECH': pu_fmt,
            'PU_DIFF': pu_fmt,
            'APLICACAO_DATA_BANCO': date_fmt,
            'OPERACAO_DATA_BRITECH': date_fmt,
            'VENCIMENTO_DATA_BANCO': date_fmt,
            'VENCIMENTO_DATA_BRITECH': date_fmt,
        }
        
        for i, col in enumerate(df.columns):
            # 3. Ajuste de Largura e Aplicação de Formato
            try:
                max_len = df[col].astype(str).str.len().max()
                width = max(len(str(col)), max_len if not pd.isna(max_len) else 0)
            except:
                width = len(str(col)) 
                
            if col in COLUNA_FORMATO:
                worksheet.set_column(i, i, width + 2, COLUNA_FORMATO[col])
            else:
                worksheet.set_column(i, i, width + 2)
            
        writer.close()
        logger.info(f"Relatório salvo com sucesso: {filename}")
        
    except Exception as e:
        logger.error(f"Falha ao salvar o arquivo Excel {filename}: {e}", exc_info=True)


def main():
    """
    Orquestra o processo de conciliação de preços unitários (PU) e gera relatórios Excel.
    """
    logger.info(f"--- 🚀 Iniciando Verificação de Inconsistências de PU ---")
    
    # LISTA DE COLUNAS ATUALIZADA COM OS NOMES CORRETOS
    COLUNAS_BANCO = ['Aplicação', 'Qtd.', 'PU Atual', 'Código', 'Vcto.', 'Valor Bruto'] 
    COLUNAS_BRITECH = ['DATA OPERAÇÃO', 'VALOR BRUTO', 'QUANTIDADE', 'DESCRIÇÃO', 'DATA VENCIMENTO'] 
    
    df_banco_clean = pd.DataFrame()
    df_britech_clean = pd.DataFrame()

    # 1. CARREGAMENTO E LIMPEZA DE DADOS
    try:
        logger.info(f"1. Processando dados do Banco (Procurando: {COLUNAS_BANCO})...")
        cleaner_banco = DataCleaner(BANCO_FILE, required_columns=COLUNAS_BANCO)
        df_banco_clean = cleaner_banco.prepare_banco_data()
        
        logger.info(f"2. Processando dados da Britech (Procurando: {COLUNAS_BRITECH})...")
        cleaner_britech = DataCleaner(BRITECH_FILE, required_columns=COLUNAS_BRITECH)
        df_britech_clean = cleaner_britech.prepare_britech_data()
        
    except Exception as e:
        logger.critical(f"\n❌ ERRO CRÍTICO DURANTE O PROCESSAMENTO DE DADOS: {e}. Encerrando.", exc_info=True)
        return

    logger.info(f" -> Dados limpos: Banco ({len(df_banco_clean)} ativos), Britech ({len(df_britech_clean)} ativos)")
    
    if df_banco_clean.empty or df_britech_clean.empty:
         logger.warning("Um dos DataFrames está vazio após a limpeza. Nenhuma conciliação será realizada.")
         return

    # 2. VERIFICAÇÃO DE CONSISTÊNCIA
    logger.info("3. Iniciando a conciliação sucessiva (Vencimento -> Aplicação)...")
    checker = ConsistencyChecker(df_banco_clean, df_britech_clean)
    
    # 3. GERAÇÃO DO RELATÓRIO COMPLETO (Matched)
    df_completo = checker.get_comparison_dataframe()
    logger.info(f"4. Conciliação concluída. {len(df_completo)} ativos conciliados.")
    save_to_excel(df_completo, OUTPUT_FILE_TOTAL, sheet_name='Comparacao_Completa')
    
    logger.info(f" -> Relatório COMPLETO (Matched) salvo em {OUTPUT_FILE_TOTAL}")
    
    # 4. IMPRIMIR AMOSTRA PARA VISUALIZAÇÃO NO TERMINAL
    if not df_completo.empty:
        logger.info("\n--- AMOSTRA DAS PRIMEIRAS 10 COMPARAÇÕES (Ordem: Maior Impacto em R$) ---")
        
        df_preview = df_completo[[
            'ASSET_ID', 
            'TIPO_ID_USADO',
            'STATUS_INCONSISTENCIA', 
            'VALOR_DIF_REAL',
            'PU_DIFF_PERC', 
            'PU_BANCO', 
            'PU_BRITECH', 
            'CODIGO_BANCO'
        ]].head(10)
        
        def format_float(x):
            if pd.isna(x): return ''
            if abs(x) > 100 or abs(x) < 1e-4:
                return f'{x:.6f}'
            else:
                return f'R$ {x:,.2f}'
                
        # Usamos print aqui apenas para formatar a tabela de visualização no terminal, que é o propósito desta seção.
        pd.set_option('display.float_format', format_float)
        print(df_preview.to_string(index=False))
        logger.info("---------------------------------------------------------")
    
    # 5. VERIFICAÇÃO E EXPORTAÇÃO DE ERROS DE PREÇO
    logger.info(f"5. Comparando {len(df_completo)} ativos com tolerância > {TOLERANCE:.2e}...")
    df_inconsistencias = checker.get_inconsistent_dataframe()
    
    # 6. GERAÇÃO DE MENSAGENS FINAIS E EXPORTAÇÃO
    if not df_inconsistencias.empty:
        save_to_excel(df_inconsistencias, OUTPUT_FILE_INCONSISTENT, sheet_name='Inconsistencias')
        
        logger.info(f"\n✅ SUCESSO! Inconsistências de Preço encontradas: {len(df_inconsistencias)}")
        logger.info(f" -> Relatório de ERROS salvo em {OUTPUT_FILE_INCONSISTENT}")
        
    else:
        logger.info("\n🎉 SUCESSO! Nenhuma inconsistência de preço maior que a tolerância foi encontrada.")
        
    logger.info(f"\n--- FIM DO PROCESSAMENTO ---")


if __name__ == "__main__":
    main()