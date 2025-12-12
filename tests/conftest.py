# tests/conftest.py

import pytest
import pandas as pd
from datetime import datetime

# Definindo a Tolerância para os testes
TOLERANCE_TEST = 1e-6 

@pytest.fixture
def tolerance():
    """Retorna a tolerância para uso nos testes de precisão."""
    return TOLERANCE_TEST

@pytest.fixture
def mock_raw_data_header_test():
    """DataFrame para testar a busca do cabeçalho na linha 3 (índice 2)."""
    
    # Linhas de "lixo"
    lixo_row_1 = pd.Series(["", "Relatório XPTO", "", "", ""], index=['A','B','C','D','E'])
    lixo_row_2 = pd.Series(["Data Base", "2024-11-21", "", "", ""], index=['A','B','C','D','E'])
    
    # Cabeçalho Real esperado pelo DataCleaner
    header_row = pd.Series(['Código', 'Aplicação', 'Qtd.', 'PU Atual', 'Vcto.', 'Valor Bruto'], index=['A','B','C','D','E', 'F'])
    
    # Linha de Dados de Teste
    data_row = pd.Series(['CDB100', datetime(2023, 1, 1), 100.0, 100.0, datetime(2025, 1, 1), 10000.00], index=header_row.index)
    
    df = pd.DataFrame([lixo_row_1, lixo_row_2, header_row, data_row])
    df.columns = [f'Col_{i}' for i in range(len(df.columns))] 
    
    return df

@pytest.fixture
def mock_banco_df():
    """DataFrame Banco Limpo Simulado (para testes de Merge/Inconsistência)"""
    data = {
        'CODIGO_BANCO': ['CDB_MATCH_VENC', 'LCA_MATCH_APL', 'DB_INCONSISTENTE', 'ATIVO_SEM_MATCH_B'],
        'APLICACAO_DATA_BANCO': [datetime(2024, 1, 1), datetime(2024, 2, 1), datetime(2024, 3, 1), datetime(2024, 4, 1)],
        'VENCIMENTO_DATA_BANCO': [datetime(2025, 1, 1), pd.NaT, datetime(2026, 1, 1), datetime(2027, 1, 1)],
        'QTD_BANCO': [100.0, 50.0, 200.0, 500.0],
        'PU_BANCO': [10.00000000, 5.00000000, 100.00001000, 10.00000000], 
        'VALOR_BRUTO_BANCO': [1000.00, 250.00, 20000.02, 5000.00]
    }
    df = pd.DataFrame(data)
    
    # Simula a geração das chaves (como DataCleaner faria)
    df['ASSET_ID_VENC'] = df['VENCIMENTO_DATA_BANCO'].dt.strftime('%Y%m%d').fillna('NULL_VENC') + '_' + df['QTD_BANCO'].astype(int).astype(str)
    df['ASSET_ID_APL'] = df['APLICACAO_DATA_BANCO'].dt.strftime('%Y%m%d').fillna('NULL_APL') + '_' + df['QTD_BANCO'].astype(int).astype(str)
    
    # Simula o TIPO_ID_USADO que o DataCleaner atribui
    df['TIPO_ID_USADO'] = 'VENCIMENTO'
    df.loc[df['ASSET_ID_VENC'].str.contains('NULL_VENC', na=False), 'TIPO_ID_USADO'] = 'APLICACAO'
    
    # O CHECKER espera que o DF tenha o índice resetado e nomeado:
    return df.reset_index(names=['index_BANCO']) 

@pytest.fixture
def mock_britech_df():
    """DataFrame Britech Limpo Simulado (para testes de Merge/Inconsistência)"""
    data = {
        'CODIGO_BRITECH': ['CDB_MATCH_VENC_B', 'LCA_MATCH_APL_B', 'DB_INCONSISTENTE_B', 'ATIVO_SEM_MATCH_BT', 'OUTRO_APL'],
        'OPERACAO_DATA_BRITECH': [datetime(2024, 1, 1), datetime(2024, 2, 1), datetime(2024, 3, 1), datetime(2024, 4, 1), datetime(2024, 2, 1)],
        'VENCIMENTO_DATA_BRITECH': [datetime(2025, 1, 1), pd.NaT, datetime(2026, 1, 1), datetime(2027, 1, 2), datetime(2028, 1, 1)],
        'QTD_BRITECH': [100.0, 50.0, 200.0, 500.0, 150.0],
        'VALOR_BRUTO_BRITECH': [1000.00, 250.00, 20000.00, 5000.00, 1500.00] 
    }
    df = pd.DataFrame(data)
    
    # Simula a geração das chaves (como DataCleaner faria)
    df['ASSET_ID_VENC'] = df['VENCIMENTO_DATA_BRITECH'].dt.strftime('%Y%m%d').fillna('NULL_VENC') + '_' + df['QTD_BRITECH'].astype(int).astype(str)
    df['ASSET_ID_APL'] = df['OPERACAO_DATA_BRITECH'].dt.strftime('%Y%m%d').fillna('NULL_APL') + '_' + df['QTD_BRITECH'].astype(int).astype(str)
    
    # Simula o TIPO_ID_USADO
    df['TIPO_ID_USADO'] = 'VENCIMENTO'
    df.loc[df['ASSET_ID_VENC'].str.contains('NULL_VENC', na=False), 'TIPO_ID_USADO'] = 'APLICACAO'
    
    # Calcula o PU Britech
    df['PU_BRITECH'] = df['VALOR_BRUTO_BRITECH'] / df['QTD_BRITECH']
    
    # O CHECKER espera que o DF tenha o índice resetado e nomeado:
    return df.reset_index(names=['index_BRITECH'])