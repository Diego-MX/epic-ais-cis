"""DX, 5 de septiembre de 2023
Módulo para generar FlatFileInfo a modo de pruebas. 
"""

meta_info = dict(option=True, vendor='fiserv')

test_info = False
lengths = (15, 10) if test_info else (1866, 1136)

cust_opts = {
    'filler'            : lengths[0], 
    'layout_ver'        : 1.0, 
    'filetype'          : 'CIS20', 
    'system_id'         : 'PMAX'}

acct_opts = {
    'filler'            : lengths[1], 
    'layout_ver'        : 1.0, 
    'filetype'          : 'AIS20', 
    'system_id'         : 'PMAX'}

head_opts = {
    'record_type'       : 'B', 
    'data_feed_sort_key':  0, 
    'append'            : -1}

foot_opts = {
    'record_type'       : 'E', 
    'data_feed_sort_key': 10**9-1, 
    'append'            : 1}

headfooters = {
    ('customer', 'header'): {**meta_info, 'data': {**cust_opts, **head_opts}}, 
    ('account' , 'header'): {**meta_info, 'data': {**acct_opts, **head_opts}},
    ('customer', 'footer'): {**meta_info, 'data': {**cust_opts, **foot_opts}},
    ('account' , 'footer'): {**meta_info, 'data': {**acct_opts, **foot_opts}}}


if __name__ == '__main__': 
    from epic_py.delta.table_info import FlatFileInfo
    import os
    choose_one = ('customer', 'header')
    flatter = FlatFileInfo.create(**headfooters[choose_one])
    print(flatter.info_to_row())
