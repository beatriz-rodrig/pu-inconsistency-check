# tests/test_processor.py

import pytest
from src.data_processor import DataCleaner, ConsistencyChecker


class MockDataCleaner(DataCleaner):
    def __init__(self, raw_data_mock, required_columns):
        self.file_path = "mock_file.xlsx"
        self.required_columns = [col.strip() for col in required_columns]
        self.df = raw_data_mock.copy()

    def _load_data(self):
        pass


# ---------- DataCleaner ----------

def test_find_header_row_success(mock_raw_data_header_test, tmp_path):
    mock_file = tmp_path / "temp_header_test.xlsx"
    mock_raw_data_header_test.to_excel(mock_file, index=False, header=False)

    required_cols = ['Código', 'Aplicação', 'Qtd.', 'PU Atual', 'Vcto.', 'Valor Bruto']
    cleaner = DataCleaner(str(mock_file), required_columns=required_cols)

    header_index = cleaner._find_header_row()
    assert header_index == 2


# ---------- ConsistencyChecker ----------

def test_merge_priority_vencimento_first(mock_banco_df, mock_britech_df):
    checker = ConsistencyChecker(mock_banco_df, mock_britech_df)
    df_merged = checker.merged_df

    assert 'CDB_MATCH_VENC' in df_merged['CODIGO_BANCO'].values
    assert (
        df_merged
        .loc[df_merged['CODIGO_BANCO'] == 'CDB_MATCH_VENC', 'TIPO_ID_USADO']
        .iloc[0] == 'VENCIMENTO'
    )


def test_merge_priority_aplicacao_fallback(mock_banco_df, mock_britech_df):
    checker = ConsistencyChecker(mock_banco_df, mock_britech_df)
    df_merged = checker.merged_df

    assert 'LCA_MATCH_APL' in df_merged['CODIGO_BANCO'].values
    assert (
        df_merged
        .loc[df_merged['CODIGO_BANCO'] == 'LCA_MATCH_APL', 'TIPO_ID_USADO']
        .iloc[0] == 'VENCIMENTO'
    )


def test_asset_without_vencimento_is_matched_by_aplicacao(
    mock_banco_df, mock_britech_df
):
    """
    Ativos sem match por VENCIMENTO devem ser conciliados
    pelo fallback de APLICAÇÃO.
    """
    checker = ConsistencyChecker(mock_banco_df, mock_britech_df)
    df_merged = checker.merged_df

    assert 'ATIVO_SEM_MATCH_B' in df_merged['CODIGO_BANCO'].values

    tipo_usado = (
        df_merged
        .loc[df_merged['CODIGO_BANCO'] == 'ATIVO_SEM_MATCH_B', 'TIPO_ID_USADO']
        .iloc[0]
    )

    assert tipo_usado == 'APLICACAO'


def test_inconsistent_dataframe_only_inconsistent_items(
    mock_banco_df, mock_britech_df, tolerance
):
    checker = ConsistencyChecker(mock_banco_df, mock_britech_df)
    df_inconsistente = checker.get_inconsistent_dataframe()

    assert len(df_inconsistente) == 1
    assert df_inconsistente['CODIGO_BANCO'].iloc[0] == 'DB_INCONSISTENTE'

    pu_diff = df_inconsistente['PU_DIFF_VALOR'].iloc[0]
    assert pu_diff > tolerance
    assert pu_diff == pytest.approx(0.00001, rel=1e-10)


def test_duplicate_keys_raise_error(mock_duplicate_keys_df, mock_britech_df):
    """Duplicidade de chave deve ser detectada."""
    with pytest.raises(ValueError):
        ConsistencyChecker(mock_duplicate_keys_df, mock_britech_df)
