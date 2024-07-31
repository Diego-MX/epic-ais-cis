"""DX: September 5th, 2023
Read specs from Excel file and upload to blob file. 
"""
# pylint: disable=invalid-name
from datetime import datetime as dt
from pytz import timezone

from dotenv import load_dotenv
import pandas as pd

from epic_py.tools import read_excel_table
from src import app_path, app_resourcer


ref_path = "refs/Security Info.xlsx.lnk"    
cols_ref = (ref_path, 'Approach', 'fraud_cols')

meta_cols = read_excel_table(*cols_ref)
meta_cols.set_index('columna')  
col_types = meta_cols['tipo'].dropna()    


def prepare_excelref(xls_df: pd.DataFrame):
    type_map = {'int': int, 'dbl': float, 'str': str}

    a_df = (xls_df[col_types.index]
            .astype(col_types.map(type_map)))
    return a_df


if __name__ == '__main__':
    tmp_path = "refs/upload-specs"  
    load_dotenv(override=True)
    
    # table: sheet
    data_ref = {
        'payments':  'PIS',
        'accounts':  'AIS',
        'customers': 'CIS'}

    a_ref, sheet = 'customers', 'CIS'
    for a_ref, sheet in data_ref.items():
        now_str = (dt.now(tz=timezone("America/Mexico_City"))
                .strftime("%Y-%m-%d_%H:%M"))

        ref_file = f"{tmp_path}/{a_ref}_cols.feather"
        blob_0   = f"{app_path}/specs/{a_ref}_specs_latest.feather"
        blob_1   = f"{app_path}/specs/{a_ref}_{now_str}.feather"

        pd_ref = read_excel_table(ref_path, sheet, a_ref)
        as_fthr = prepare_excelref(pd_ref)
        as_fthr.to_feather(ref_file)

        app_resourcer.upload_storage_blob(
            ref_file, blob_0, 'gold', overwrite=True, verbose=1)
        app_resourcer.upload_storage_blob(
            ref_file, blob_1, 'gold', overwrite=True, verbose=1)
