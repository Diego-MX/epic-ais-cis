from epic_py.delta import FlatFileInfo

meta_info = dict(option=True, vendor='fiserv')

cust_opts = {
    'filetype'          : 'CIS20', 
    'system_id'         : 'PMAX', 
    'layout_ver'        : 1.0, 
    'filler'            : 1866}

acct_opts = {
    'system_id'         : 'PMAX',
    'filetype'          : 'AIS20', 
    'layout_ver'        : 1.0, 
    'filler'            : 1136}

head_opts = {
    'record_type'       : 'B', 
    'data_feed_sort_key': 0, 
    'append'            : -1}

foot_opts = {
    'record_type'       : 'E', 
    'data_feed_sort_key': 10**9-1, 
    'append'            : 1}

headfooters = {
    ('customer', 'header'): {**meta_info, 'data': {**cust_opts, **head_opts}}, 
    ('account' , 'header'): {**meta_info, 'data': {**acct_opts, **head_opts}},
    ('customer', 'footer'): {**meta_info, 'data': {**cust_opts, **foot_opts}},
    ('account' , 'footer'): {**meta_info, 'data': {**acct_opts, **foot_opts}}
}
