
from epic_py import TypeHandler


fiserv_types = {  
    'date' : { 'format': 'yyyyMMdd'}, 
    'ts'   : { 'format': 'hhmmss'}
}




fiserv_handler = TypeHandler(fiserv_types)