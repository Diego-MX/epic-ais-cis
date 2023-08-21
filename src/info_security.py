from datetime import datetime as dt
import pandas as pd
from pytz import timezone

from epic_py.tools import read_excel_table


ref_path = "refs/Security Info.xlsx.lnk"
cols_ref = (ref_path, 'Approach', 'fraud_cols')

meta_cols = read_excel_table(*cols_ref).set_index('columna')
col_types = meta_cols['tipo'].dropna()


def prepare_excelref(xls_df: pd.DataFrame):
    type_map = {'int': int, 'dbl': float, 'str': str}
    
    a_df = (xls_df[col_types.index]
            .astype(col_types.map(type_map)))
    return a_df         


if __name__ == '__main__': 
    from config import app_resourcer, blob_path
    tmp_path = "refs/upload-specs"
    
    # table: sheet
    data_ref = {
        'payments':  'Payment', 
        'accounts':  'Account', 
        'customers': 'Customer'}

    a_ref, sheet = 'payments', 'Payment'
    for a_ref, sheet in data_ref.items(): 
        now_str = dt.now(tz=timezone("America/Mexico_City")).strftime("%Y-%m-%d_%H:%M")

        ref_file = f"{tmp_path}/{a_ref}_cols.feather"
        blob_0   = f"{blob_path}/specs/{a_ref}_specs_latest.feather"
        blob_1   = f"{blob_path}/specs/{a_ref}_{now_str}.feather"

        pd_ref = read_excel_table(ref_path, sheet, a_ref)
        as_fthr = prepare_excelref(pd_ref)
        as_fthr.to_feather(ref_file)
         
        #app_resourcer.upload_storage_blob(ref_file, blob_0, 'gold', overwrite=True, verbose=1)
        #app_resourcer.upload_storage_blob(ref_file, blob_1, 'gold', overwrite=True, verbose=1)
