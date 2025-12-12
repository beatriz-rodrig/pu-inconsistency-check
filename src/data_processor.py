# src/data_processor.py

import pandas as pd
from typing import List
import os
import logging # NOVO IMPORT

# Define a tolerância para a inconsistência de PU
TOLERANCE = 1e-6
logger = logging.getLogger(__name__) # Obtém o logger configurado no main.py

# --- CLASSE DATACLEANER ---
class DataCleaner:
    """
    Responsável por carregar, localizar o cabeçalho e limpar os dados de entrada.
    """
    def __init__(self, file_path: str, required_columns: List[str]):
        self.file_path = file_path
        self.required_columns = [col.strip() for col in required_columns] 
        self.df = self._load_data()
        
    def _find_header_row(self) -> int:
        MAX_ROWS_TO_CHECK = 50 
        file_name = self.file_path.split(os.sep)[-1]
        
        try:
            df_raw = pd.read_excel(self.file_path, header=None, nrows=MAX_ROWS_TO_CHECK)
        except Exception as e:
            logger.error(f"[{file_name}] Erro ao tentar pré-carregar as primeiras linhas: {e}", exc_info=True)
            raise Exception(f"Erro ao tentar pré-carregar as primeiras linhas do Excel: {e}")
        
        num_rows = len(df_raw)
        if num_rows == 0: 
            logger.warning(f"[{file_name}] O arquivo Excel parece estar vazio ou não contém dados válidos.")
            raise ValueError("O arquivo Excel parece estar vazio ou não contém dados válidos.")
            
        for i in range(num_rows):
            current_header = df_raw.iloc[i].astype(str).str.strip().tolist()
            if all(col in current_header for col in self.required_columns): 
                logger.info(f"[{file_name}] Cabeçalho encontrado no índice de linha {i}.")
                return i
                
        logger.warning(f"[{file_name}] A linha de cabeçalho não foi encontrada nas {MAX_ROWS_TO_CHECK} linhas inspecionadas.")
        return -1

    def _load_data(self) -> pd.DataFrame:
        file_name = self.file_path.split(os.sep)[-1]
        try:
            header_index = self._find_header_row()
            if header_index == -1: 
                raise ValueError(f"A linha de cabeçalho não foi encontrada nas 50 linhas inspecionadas. Colunas necessárias: {self.required_columns}")
            
            df = pd.read_excel(self.file_path, header=header_index)
            df.columns = df.columns.str.strip()
            logger.info(f"[{file_name}] Dados carregados com sucesso (header index: {header_index}). Total de linhas brutas: {len(df)}")
            return df
        except Exception as e:
            logger.error(f"[{file_name}] Erro ao carregar e processar o arquivo: {e}", exc_info=True)
            raise Exception(f"Erro ao carregar e processar o arquivo: {e}")

    def prepare_banco_data(self) -> pd.DataFrame:
        """ Prepara os dados do Banco, cria as chaves de conciliação VENCIMENTO e APLICACAO. """
        COL_VALOR_BRUTO, COL_CODIGO, COL_APLICACAO, COL_QTD, COL_PU, COL_VENCIMENTO = 'Valor Bruto','Código', 'Aplicação', 'Qtd.', 'PU Atual', 'Vcto.' 
        COLUNAS_BANCO_MANTER = [COL_VALOR_BRUTO, COL_CODIGO, COL_APLICACAO, COL_QTD, COL_PU, COL_VENCIMENTO]
        
        try: df_banco = self.df[COLUNAS_BANCO_MANTER].copy()
        except KeyError as e: 
            logger.error(f"[Banco] Erro de Coluna (KeyError): {e}", exc_info=True)
            raise KeyError(f"Erro de Coluna no Extrato do Banco: {e}")

        # Conversão de Tipos
        df_banco[COL_APLICACAO] = pd.to_datetime(df_banco[COL_APLICACAO], errors='coerce')
        df_banco[COL_VENCIMENTO] = pd.to_datetime(df_banco[COL_VENCIMENTO], errors='coerce')
        df_banco['PU_BANCO'] = pd.to_numeric(df_banco[COL_PU], errors='coerce')
        df_banco['QTD_BANCO'] = pd.to_numeric(df_banco[COL_QTD], errors='coerce')
        df_banco['VALOR_BRUTO_BANCO'] = pd.to_numeric(df_banco[COL_VALOR_BRUTO], errors='coerce')

        # Criação de Chaves
        df_banco['APLICACAO_PAD'] = df_banco[COL_APLICACAO].dt.strftime('%Y%m%d').fillna('NULL_APL')
        df_banco['VENCIMENTO_PAD'] = df_banco[COL_VENCIMENTO].dt.strftime('%Y%m%d').fillna('NULL_VENC')
        df_banco['QTD_STR'] = df_banco[COL_QTD].astype(str).str.replace(r'\.0+$', '', regex=True).str.strip() 
        
        df_banco['ASSET_ID_VENC'] = df_banco['VENCIMENTO_PAD'] + '_' + df_banco['QTD_STR']
        df_banco['ASSET_ID_APL'] = df_banco['APLICACAO_PAD'] + '_' + df_banco['QTD_STR']
        
        # Definição de Prioridade de Chave
        df_banco['ASSET_ID'] = df_banco['ASSET_ID_VENC']
        df_banco['TIPO_ID_USADO'] = 'VENCIMENTO'
        df_banco.loc[df_banco['ASSET_ID'].str.contains('NULL_VENC', na=False), 'TIPO_ID_USADO'] = 'APLICACAO'
        
        # Finalização e Renomeação
        df_banco.rename(columns={
            COL_CODIGO: 'CODIGO_BANCO', 
            COL_APLICACAO: 'APLICACAO_DATA_BANCO',
            COL_VENCIMENTO: 'VENCIMENTO_DATA_BANCO'
        }, inplace=True)
        
        COLS_DROP = ['APLICACAO_PAD', 'QTD_STR', 'VENCIMENTO_PAD', COL_PU, COL_QTD]
        COLUNAS_SAIDA = [col for col in df_banco.columns if col not in COLS_DROP]
        
        df_final = df_banco.dropna(subset=['PU_BANCO'])[COLUNAS_SAIDA].copy()
        logger.info(f"[Banco] Preparação de dados finalizada. Ativos válidos para conciliação: {len(df_final)}")
        return df_final


    def prepare_britech_data(self) -> pd.DataFrame:
        """ Prepara os dados da Britech, cria as chaves de conciliação VENCIMENTO e APLICACAO. """
        COL_DESC, COL_OPERACAO, COL_VALOR, COL_QTD, COL_VENCIMENTO = 'DESCRIÇÃO', 'DATA OPERAÇÃO', 'VALOR BRUTO', 'QUANTIDADE', 'DATA VENCIMENTO' 
        COLUNAS_BRITECH_MANTER = [COL_DESC, COL_OPERACAO, COL_VALOR, COL_QTD, COL_VENCIMENTO]
        
        try: df_britech = self.df[COLUNAS_BRITECH_MANTER].copy()
        except KeyError as e: 
            logger.error(f"[Britech] Erro de Coluna (KeyError): {e}", exc_info=True)
            raise KeyError(f"Erro de Coluna no Extrato da Britech: {e}")
            
        # Conversão e Limpeza de Dados
        df_britech[COL_OPERACAO] = pd.to_datetime(df_britech[COL_OPERACAO], errors='coerce')
        df_britech[COL_VENCIMENTO] = pd.to_datetime(df_britech[COL_VENCIMENTO], errors='coerce')
        
        # Remoção de R$ e Substituição de vírgula por ponto para conversão numérica
        df_britech[COL_VALOR] = pd.to_numeric(df_britech[COL_VALOR].astype(str).str.replace(r'[R$]', '', regex=True).str.replace(',', '.', regex=False), errors='coerce')
        df_britech[COL_QTD] = pd.to_numeric(df_britech[COL_QTD], errors='coerce')
        
        # Filtro de linhas inválidas (QTD e Valor nulos/zero)
        df_britech = df_britech[(df_britech[COL_QTD].notna()) & (df_britech[COL_QTD] != 0) & (df_britech[COL_VALOR].notna())].copy()

        # Criação de Chaves
        df_britech['APLICACAO_PAD'] = df_britech[COL_OPERACAO].dt.strftime('%Y%m%d').fillna('NULL_APL')
        df_britech['VENCIMENTO_PAD'] = df_britech[COL_VENCIMENTO].dt.strftime('%Y%m%d').fillna('NULL_VENC')
        df_britech['QTD_STR'] = df_britech[COL_QTD].astype(str).str.replace(r'\.0+$', '', regex=True).str.strip()
        
        df_britech['ASSET_ID_VENC'] = df_britech['VENCIMENTO_PAD'] + '_' + df_britech['QTD_STR']
        df_britech['ASSET_ID_APL'] = df_britech['APLICACAO_PAD'] + '_' + df_britech['QTD_STR']

        # Cálculo do PU (Preço Unitário)
        df_britech['PU_BRITECH'] = df_britech[COL_VALOR] / df_britech[COL_QTD]
        
        # Finalização e Renomeação
        df_britech.rename(columns={
            COL_DESC: 'CODIGO_BRITECH',
            COL_OPERACAO: 'OPERACAO_DATA_BRITECH',
            COL_VALOR: 'VALOR_BRUTO_BRITECH',
            COL_VENCIMENTO: 'VENCIMENTO_DATA_BRITECH',
            COL_QTD: 'QTD_BRITECH'
        }, inplace=True)
        
        COLS_DROP = ['APLICACAO_PAD', 'QTD_STR', 'VENCIMENTO_PAD']
        COLUNAS_SAIDA = [col for col in df_britech.columns if col not in COLS_DROP]
        
        df_final = df_britech[COLUNAS_SAIDA].copy()
        logger.info(f"[Britech] Preparação de dados finalizada. Ativos válidos para conciliação: {len(df_final)}")
        return df_final


# --- CLASSE CONSISTENCYCHECKER ---
class ConsistencyChecker:
    """
    Responsável por unir os dados e identificar as inconsistências, usando as 2 chaves de conciliação.
    """
    def __init__(self, df_banco: pd.DataFrame, df_britech: pd.DataFrame):
        # AQUI PRECISAMOS REINICIAR OS ÍNDICES SE ELES NÃO TIVEREM SIDO RESETADOS NA PREPARAÇÃO
        # Assumindo que você manteve o reset_index do teste, vamos garantir que o df_banco/df_britech 
        # tenham um índice sequencial para o merge. 
        self.df_banco = df_banco.reset_index(drop=True)
        self.df_britech = df_britech.reset_index(drop=True)
        self.merged_df = self._merge_data_successive()

    def _merge_data_successive(self) -> pd.DataFrame:
        """ 
        Realiza o MERGE sucessivo (Vencimento -> Aplicação) para capturar o máximo de ativos conciliados.
        """
        keys = {
            'VENCIMENTO': 'ASSET_ID_VENC', 
            'APLICACAO': 'ASSET_ID_APL'
        }
        
        # Usamos sets para armazenar as chaves de conciliação que já deram match
        conciliated_ids = set()
        df_conciliated = pd.DataFrame()
        
        for tipo, key in keys.items():
            
            # Filtra os DFs para manter apenas os ativos que AINDA NÃO FORAM CONCILIADOS
            # O filtro é feito pelo ASSET_ID_VENC, que é a chave que define a prioridade de conciliação
            # (mesmo que o match seja feito por ASSET_ID_APL no segundo loop)
            df_banco_pending = self.df_banco[~self.df_banco['ASSET_ID_VENC'].isin(conciliated_ids)].copy()
            df_britech_pending = self.df_britech[~self.df_britech['ASSET_ID_VENC'].isin(conciliated_ids)].copy()
            
            current_merge = pd.merge(
                df_banco_pending, 
                df_britech_pending, 
                left_on=key, 
                right_on=key, 
                how='inner', 
                suffixes=('_BANCO', '_BRITECH') 
            )
            
            if not current_merge.empty:
                logger.info(f"[Merge] {len(current_merge)} ativos conciliados por {tipo}.")
                current_merge['ASSET_ID'] = current_merge[key]
                current_merge['TIPO_ID_USADO'] = tipo
                
                # Atualiza os sets de IDs conciliados usando a chave VENCIMENTO (prioritária)
                if tipo == 'VENCIMENTO':
                    conciliated_ids.update(current_merge[key].unique())
                else: 
                    # Se conciliou por APLICAÇÃO, usamos a chave ASSET_ID_VENC_BANCO para evitar que 
                    # esse ativo seja considerado novamente em merges futuros (embora não deva haver)
                    conciliated_ids.update(current_merge['ASSET_ID_VENC_BANCO'].unique())
                    
                df_conciliated = pd.concat([df_conciliated, current_merge], ignore_index=True)

        cols_to_keep_final = [
            'ASSET_ID', 'TIPO_ID_USADO', 'CODIGO_BANCO', 'APLICACAO_DATA_BANCO', 'VENCIMENTO_DATA_BANCO', 'QTD_BANCO', 'PU_BANCO', 'VALOR_BRUTO_BANCO',
            'CODIGO_BRITECH', 'OPERACAO_DATA_BRITECH', 'VENCIMENTO_DATA_BRITECH', 'QTD_BRITECH', 'PU_BRITECH', 'VALOR_BRUTO_BRITECH'
        ]

        df_final = df_conciliated.reindex(columns=cols_to_keep_final).dropna(subset=['ASSET_ID']).drop_duplicates(subset=['ASSET_ID'])
        
        return df_final


    def get_comparison_dataframe(self) -> pd.DataFrame:
        """ Cria o DataFrame de comparação com cálculo de diferenças de PU e Valor. """
        df = self.merged_df.copy()
        
        COLUNAS_ORGANIZADAS_ESQUEMA = [
            'ASSET_ID', 'TIPO_ID_USADO', 'STATUS_INCONSISTENCIA', 'VALOR_DIF_REAL', 'PU_DIFF_VALOR', 'PU_DIFF_PERC', 'CODIGO_BANCO', 'CODIGO_BRITECH', 
            'APLICACAO_DATA_BANCO', 'VENCIMENTO_DATA_BANCO', 'QTD_BANCO', 'VALOR_BRUTO_BANCO', 'PU_BANCO',
            'OPERACAO_DATA_BRITECH', 'VENCIMENTO_DATA_BRITECH', 'QTD_BRITECH', 'VALOR_BRUTO_BRITECH', 'PU_BRITECH', 'PU_DIFF'
        ]

        if df.empty: 
            logger.warning("Nenhum ativo foi conciliado. Retornando DataFrame de comparação vazio.")
            return pd.DataFrame(columns=COLUNAS_ORGANIZADAS_ESQUEMA)

        df['PU_DIFF'] = df['PU_BANCO'] - df['PU_BRITECH'] 
        df['PU_DIFF_VALOR'] = df['PU_DIFF'].abs() 
        # Adicionamos 1e-12 ao divisor para evitar divisão por zero, caso PU_BRITECH seja zero
        df['PU_DIFF_PERC'] = (df['PU_DIFF_VALOR'] / (df['PU_BRITECH'].abs() + 1e-12))
        df['STATUS_INCONSISTENCIA'] = df['PU_DIFF_VALOR'] > TOLERANCE
        
        df['VALOR_DIF_REAL'] = (df['VALOR_BRUTO_BANCO'] - df['VALOR_BRUTO_BRITECH']).abs() 
        
        df_saida = df.reindex(columns=COLUNAS_ORGANIZADAS_ESQUEMA).sort_values(by='VALOR_DIF_REAL', ascending=False)
        return df_saida.reset_index(drop=True)

    def get_inconsistent_dataframe(self) -> pd.DataFrame:
        """ Retorna apenas os ativos conciliados com inconsistência de PU/Valor. """
        df_completo = self.get_comparison_dataframe()
        return df_completo[df_completo['STATUS_INCONSISTENCIA'] == True].copy()
        
    def get_non_matched_dataframe(self) -> pd.DataFrame: 
        """ Mantido como um método, mas retorna um DataFrame vazio conforme escopo final. """
        COLUNAS_SAIDA = [
            'ASSET_ID', 'STATUS_CONCILIACAO', 'TIPO_ID_USADO', 'CODIGO_BANCO', 'APLICACAO_DATA_BANCO', 'VENCIMENTO_DATA_BANCO',
            'QTD_BANCO', 'PU_BANCO', 'VALOR_BRUTO_BANCO', 'CODIGO_BRITECH', 'OPERACAO_DATA_BRITECH', 
            'VENCIMENTO_DATA_BRITECH', 'QTD_BRITECH', 'PU_BRITECH', 'VALOR_BRUTO_BRITECH'
        ]
        return pd.DataFrame(columns=COLUNAS_SAIDA)