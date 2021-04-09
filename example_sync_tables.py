from x_x import account_info as ai

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
    

