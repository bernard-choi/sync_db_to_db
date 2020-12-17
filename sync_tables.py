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
    table_list.append({
        'acc_from' : ai.analdb_sql_info(),
        'acc_to' : ai.vmdb_sql_info(),
        'db_from': 'ople',
        'table_from' : 'rest_suppliergroup',
        'db_to' : 'ople',
        'table_to' : 'rest_suppliergroup',
        'primary_key' : 'id'
    })
    table_list.append({
        'acc_from' : ai.analdb_sql_info(),
        'acc_to' : ai.vmdb_sql_info(),
        'db_from': 'ople',
        'table_from' : 'rest_order',
        'db_to' : 'ople',
        'table_to' : 'rest_order',
        'primary_key' : 'id'
    })
    table_list.append({
        'acc_from' : ai.analdb_sql_info(),
        'acc_to' : ai.vmdb_sql_info(),
        'db_from': 'ople',
        'table_from' : 'rest_supplier',
        'db_to' : 'ople',
        'table_to' : 'rest_supplier',
        'primary_key' : 'id'
    })
    table_list.append({
        'acc_from' : ai.analdb_sql_info(),
        'acc_to' : ai.vmdb_sql_info(),
        'db_from': 'ople',
        'table_from' : 'rest_newstore',
        'db_to' : 'ople',
        'table_to' : 'rest_newstore',
        'primary_key' : 'id'
    })
    table_list.append({
        'acc_from' : ai.analdb_sql_info(),
        'acc_to' : ai.vmdb_sql_info(),
        'db_from': 'ople',
        'table_from' : 'rest_mylist',
        'db_to' : 'ople',
        'table_to' : 'rest_mylist',
        'primary_key' : 'id'
    })
    table_list.append({
        'acc_from' : ai.analdb_sql_info(),
        'acc_to' : ai.vmdb_sql_info(),
        'db_from': 'ople',
        'table_from' : 'rest_accounthistory',
        'db_to' : 'ople',
        'table_to' : 'rest_accounthistory',
        'primary_key' : 'id'
    })
    table_list.append({
        'acc_from' : ai.analdb_sql_info(),
        'acc_to' : ai.vmdb_sql_info(),
        'db_from': 'ople',
        'table_from' : 'rest_searchlog',
        'db_to' : 'ople',
        'table_to' : 'rest_searchlog',
        'primary_key' : 'id'
    })
    '''
    servicedb sync **
    '''
    table_list.append({
        'acc_from' : ai.servicedb_sql_info(),
        'acc_to' : ai.vmdb_sql_info(),
        'db_from' : 'ople',
        'table_from' : 'xprd_master_businesscategoryproduct',
        'db_to' : 'ople',
        'table_to' : 'xprd_master_businesscategoryproduct',
        'primary_key' : 'id'        
    })
    
    table_list.append({
        'acc_from' : ai.servicedb_sql_info(),
        'acc_to' : ai.vmdb_sql_info(),
        'db_from' : 'ople',
        'table_from' : 'xprd_master_businesscategory',
        'db_to' : 'ople',
        'table_to' : 'xprd_master_businesscategory',
        'primary_key' : 'id'        
    })
    
    
    ## prd_master2        
    table_list.append({
        'acc_from' : ai.analdb_sql_info(),
        'acc_to' : ai.vmdb_sql_info(),
        'db_from': 'prd_master2',
        'table_from' : 'xprd_master_source',
        'db_to' : 'prd_master2',
        'table_to' : 'xprd_master_source',
        'primary_key' : None
    })           
    
    #data_sc
    table_list.append({
        'acc_from' : ai.analdb_sql_info(),
        'acc_to' : ai.vmdb_sql_info(),
        'db_from': 'data_sc',
        'table_from' : 'mylist_log',
        'db_to' : 'data_sc',
        'table_to' : 'mylist_log',
        'primary_key' : None
    })    
    table_list.append({
        'acc_from' : ai.analdb_sql_info(),
        'acc_to' : ai.vmdb_sql_info(),
        'db_from': 'data_sc',
        'table_from' : 'searchlog_parsed',
        'db_to' : 'data_sc',
        'table_to' : 'mylist_log',
        'primary_key' : None
    })
    table_list.append({
        'acc_from' : ai.analdb_sql_info(),
        'acc_to' : ai.vmdb_sql_info(),
        'db_from': 'data_sc',
        'table_from' : 'searchlog_parsed',
        'db_to' : 'data_sc',
        'table_to' : 'searchlog_parsed',
        'primary_key' : None
    })        
    
    # sales_admin
    table_list.append({
        'acc_from' : ai.analdb_sql_info(),
        'acc_to' : ai.vmdb_sql_info(),
        'db_from': 'sales_admin',
        'table_from' : 'stores_storemodel',
        'db_to' : 'sales_admin',
        'table_to' : 'stores_storemodel',
        'primary_key' : 'id'
    })
    

class Infor_urgent:        
    table_list = []    
    
    table_list.append({
        'acc_from' : ai.servicedb_sql_info(),
        'acc_to' : ai.vmdb_sql_info(),
        'db_from' : 'ople',
        'table_from' : 'xprd_master_businesscategoryproduct',
        'db_to' : 'ople',
        'table_to' : 'xprd_master_businesscategoryproduct',
        'primary_key' : 'id'        
    })
    
    table_list.append({
        'acc_from' : ai.servicedb_sql_info(),
        'acc_to' : ai.vmdb_sql_info(),
        'db_from' : 'ople',
        'table_from' : 'xprd_master_businesscategory',
        'db_to' : 'ople',
        'table_to' : 'xprd_master_businesscategory',
        'primary_key' : 'id'        
    })

