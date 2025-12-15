import pandas as pd
import logging

logger = logging.getLogger(__name__)


def save_to_excel(df: pd.DataFrame, filename: str, sheet_name: str):
    try:
        writer = pd.ExcelWriter(filename, engine='xlsxwriter', datetime_format='yyyy-mm-dd')
        df.to_excel(writer, sheet_name=sheet_name, index=False)

        workbook = writer.book
        worksheet = writer.sheets[sheet_name]

        currency_fmt = workbook.add_format({'num_format': 'R$ #,##0.00'})
        percentage_fmt = workbook.add_format({'num_format': '0.00%'})
        pu_fmt = workbook.add_format({'num_format': '0.0000000000'})
        date_fmt = workbook.add_format({'num_format': 'dd-mm-yyyy'})

        COLUMN_FORMATS = {
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
            try:
                max_len = df[col].astype(str).str.len().max()
                width = max(len(str(col)), max_len if not pd.isna(max_len) else 0)
            except Exception:
                width = len(str(col))

            worksheet.set_column(i, i, width + 2, COLUMN_FORMATS.get(col))

        writer.close()
        logger.info(f"Relatório salvo com sucesso: {filename}")

    except Exception as e:
        logger.error(f"Erro ao salvar Excel {filename}: {e}", exc_info=True)