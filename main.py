#!/usr/bin/env python
# coding: utf-8
import datetime
import logging
import sys

import pandas as pd

from pyjin import pyjin
from x_x import account_info as ai

import sync_tables
from module import all_delete_insert
from module import delete_upsert
from module import create_table

def get_mode(acc_from, acc_to, db_from, db_to, table_from, table_to, primary_key: str):
        
    with pyjin.connectDB(**acc_from, engine_type='NullPool') as con:         
        pyjin.execute_query(con,"SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")                
                
        ## 테이블이 존재하지 않을경우
        if not pyjin.check_is_table(acc=acc_to, 
                                    table_name = table_to,
                                    schema_name = db_to):
            
            return 'no_table'
        
        columns_df_from =pyjin.execute_query(con,
                            """
                            select * from {}.{} limit 1
                            """.format(db_from, table_from)
                            , output='df').columns.tolist()                
        
        if primary_key is None:            
            return primary_key
        elif primary_key in columns_df_from and 'update_date' in columns_df_from:
            return '{},update_date'.format(primary_key)        
        elif primary_key in columns_df_from:
            return primary_key
        
        pyjin.execute_query(con,"SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")    


def main(mode):           
    table_list = sync_tables.Infor.table_list if mode == 0 else sync_tables.Infor_urgent.table_list    
     
    for row in table_list:  
        pyjin.print_logging('{} table sync...'.format(row['table_from']))
                        
        '''
        when no col_matching, default value is to
        '''       
        row['col_matching'] = row.get('col_matching','to')
        
        try:               
            '''    
            id, update_date 있으면 delete, update, insert 고려 (mode='id, update_date')
            id 만 있으면 delete insert 고려 (mode='id')
            둘다 없으면 그냥 테이블 싹지우고 재생성 (mode = None)
            '''                                        
            mode = get_mode(acc_from = row['acc_from'],
                            acc_to = row['acc_to'],
                            db_from = row['db_from'],
                            db_to = row['db_to'],
                            table_from = row['table_from'],
                            table_to=row['table_to'],
                            primary_key = row['primary_key'])                                            
            pyjin.print_logging('mode is {}'.format(mode))
                    
            if mode == 'no_table':
                print('create_table start')
                create_table.main(acc_from = row['acc_from'],
                            acc_to = row['acc_to'],
                            db_from = row['db_from'],
                            db_to = row['db_to'],
                            table_from = row['table_from'],
                            table_to = row['table_to'],
                            primary_key = row['primary_key'])
                        
            elif mode is not None:            
                print('delete_upsert start')
                delete_upsert.main(acc_from = row['acc_from'], 
                                acc_to = row['acc_to'],
                                db_from = row['db_from'],
                                db_to = row['db_to'],
                                table_from = row['table_from'],
                                table_to= row['table_to'],                               
                                mode = mode,
                                primary_key=row['primary_key'],
                                col_matching= row['col_matching'])
                                            
            elif mode is None:
                print('all_delete_insert start')
                all_delete_insert.main(acc_from = row['acc_from'], 
                                        acc_to = row['acc_to'],
                                        db_from = row['db_from'],
                                        db_to = row['db_to'],
                                        table_from = row['table_from'],
                                        table_to= row['table_to'],
                                        col_matching = row['col_matching'])
                        
            pyjin.print_logging('completed')  
                
        except Exception as e:
            pyjin.print_logging("failed, error: {}".format(e))            

if __name__ == "__main__":  
    '''
    mode 0 -> non_urgent_mode
    mode 1 -> urgent mode
    '''        
    mode = sys.argv[1] if len(sys.argv) >1 else 0
    main(mode)
    
    
