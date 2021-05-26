import sys

from . import account_info as ai
from . import sync_db_to_db

class Infor:        
    table_list = []
        
    ## ople
    table_list.append({
        'acc_from' : ai.analdb_sql_info(),
        'acc_to' : ai.vmdb_sql_info(),
        'db_from': 'ople',
        'table_from' : 'rest_product',
        'db_to' : 'ople',
        'table_to' : 'rest_product',
        'primary_key' : 'id'
    })
    
    table_list.append({
        'acc_from' : ai.analdb_sql_info(),
        'acc_to' : ai.vmdb_sql_info(),
        'db_from': 'ople',
        'table_from' : 'rest_store',
        'db_to' : 'ople',
        'table_to' : 'rest_store',
        'primary_key' : 'id'
    })


class Infor_urgent:        
    table_list = []    
    
    table_list.append({
        'acc_from' : ai.analdb_sql_info(),
        'acc_to' : ai.vmdb_sql_info(),
        'db_from': 'ople',
        'table_from' : 'rest_oplemember',
        'db_to' : 'ople',
        'table_to' : 'rest_oplemember',
        'primary_key' : 'id'
    })
            

if __name__ =='__main__':
    
    '''
    mode 0 -> non_urgent_mode
    mode 1 -> urgent mode
    '''
             
    batch_mode = sys.argv[1] if len(sys.argv) >1 else 0
    list_infor = Infor.table_list if batch_mode == 0 else Infor_temp.table_list
    
    for dict_infor in list_infor:        
        sync_db_to_db.main(dict_infor)