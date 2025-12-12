# tests/test_processor.py

import pandas as pd
import pytest
from src.data_processor import DataCleaner, ConsistencyChecker, TOLERANCE

# Para fins de teste, criamos uma classe mockada para simular o comportamento de DataCleaner
class MockDataCleaner(DataCleaner):
    def __init__(self, raw_data_mock, required_columns):
        self.file_path = "mock_file.xlsx"
        self.required_columns = [col.strip() for col in required_columns]
        self.df = raw_data_mock.copy()
    
    def _load_data(self):
        pass

# --- 1. Testes da Classe DataCleaner ---

def test_find_header_row_success(mock_raw_data_header_test, tmp_path):
    """Testa se o DataCleaner encontra o cabeçalho correto (índice 2)."""
    
    # Prepara um arquivo mock no diretório temporário
    mock_file = tmp_path / "temp_header_test.xlsx"
    mock_raw_data_header_test.to_excel(mock_file, index=False, header=False)
    
    required_cols = ['Código', 'Aplicação', 'Qtd.', 'PU Atual', 'Vcto.', 'Valor Bruto']
    
    cleaner = DataCleaner(str(mock_file), required_columns=required_cols)
    header_index = cleaner._find_header_row()
    
    assert header_index == 2
    

# --- 2. Testes da Classe ConsistencyChecker ---

def test_merge_priority_vencimento_first(mock_banco_df, mock_britech_df):
    """Testa se o primeiro ativo (CDB) é conciliado por ASSET_ID_VENC."""
    
    checker = ConsistencyChecker(mock_banco_df, mock_britech_df)
    df_merged = checker.merged_df
    
    # O ativo 'CDB_MATCH_VENC' tem match por VENCIMENTO
    assert 'CDB_MATCH_VENC' in df_merged['CODIGO_BANCO'].values
    assert df_merged.loc[df_merged['CODIGO_BANCO'] == 'CDB_MATCH_VENC', 'TIPO_ID_USADO'].iloc[0] == 'VENCIMENTO'
    

def test_merge_priority_aplicacao_fallback(mock_banco_df, mock_britech_df):
    """
    Testa se o ativo 'LCA_MATCH_APL' (Vencimentos nulos nos 2 DFs) é conciliado, 
    sendo marcado como VENCIMENTO pelo merge prioritário.
    """
    
    checker = ConsistencyChecker(mock_banco_df, mock_britech_df)
    df_merged = checker.merged_df
    
    # Ativo 'LCA_MATCH_APL' deve estar conciliado
    assert 'LCA_MATCH_APL' in df_merged['CODIGO_BANCO'].values
    
    # A lógica de merge sobrescreve para 'VENCIMENTO' se o match ocorreu no primeiro loop (ASSET_ID_VENC).
    assert df_merged.loc[df_merged['CODIGO_BANCO'] == 'LCA_MATCH_APL', 'TIPO_ID_USADO'].iloc[0] == 'VENCIMENTO'


def test_inconsistent_dataframe_only_inconsistent_items(mock_banco_df, mock_britech_df, tolerance):
    """Testa se get_inconsistent_dataframe retorna APENAS o ativo com PU_DIFF > TOLERANCE."""
    
    checker = ConsistencyChecker(mock_banco_df, mock_britech_df)
    df_inconsistente = checker.get_inconsistent_dataframe()
    
    # Apenas o ativo 'DB_INCONSISTENTE' (PU_BANCO=100.00001 vs PU_BRITECH=100.00000) deve ser retornado
    assert len(df_inconsistente) == 1
    assert df_inconsistente['CODIGO_BANCO'].iloc[0] == 'DB_INCONSISTENTE'
    
    # Verifica a condição de inconsistência
    pu_diff = df_inconsistente['PU_DIFF_VALOR'].iloc[0]
    assert pu_diff > tolerance
    assert pu_diff == pytest.approx(0.00001, rel=1e-10) # 1e-5