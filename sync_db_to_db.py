#!/usr/bin/env python
# coding: utf-8

import traceback
import datetime
import sys

import pandas as pd

from .pyjin import pyjin
from .module import all_delete_insert
from .module import delete_upsert
from .module import create_table
from .module import db_module

def get_mode(acc_from, 
             acc_to, 
             db_from, 
             db_to, 
             table_from, 
             table_to, 
             primary_key: str):
    
    ## 테이블이 존재하지 않을경우
    if not pyjin.check_is_table(acc=acc_to, 
                                table_name = table_to,
                                schema_name = db_to):
        
        return 'no_table'    
        
    with pyjin.connectDB(**acc_from, engine_type='NullPool') as con:         
        pyjin.execute_query(con,"SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
        columns_df_from =pyjin.execute_query(con,
                            """
                            select * from {}.{} limit 1
                            """.format(db_from, table_from)
                            , output='df').columns.tolist()                
        pyjin.execute_query(con,"SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")         
        
        
    if primary_key is None:            
        return primary_key
    elif primary_key in columns_df_from and 'update_date' in columns_df_from:
        return '{},update_date'.format(primary_key)        
    elif primary_key in columns_df_from:
        return primary_key    


def main(**dict_infor):
    pyjin.print_logging('{}.{} table sync...'.format(dict_infor['db_from'], dict_infor['table_from']))
                    
    '''
    when no col_matching, default value is to
    '''       
    dict_infor['col_matching'] = dict_infor.get('col_matching','to')
    
    try:               
        '''
        id, update_date 있으면 delete, update, insert 고려 (mode='id, update_date')
        id 만 있으면 delete insert 고려 (mode='id')
        둘다 없으면 그냥 테이블 싹지우고 재생성 (mode = None)
        '''
        mode = get_mode(acc_from = dict_infor['acc_from'],
                        acc_to = dict_infor['acc_to'],
                        db_from = dict_infor['db_from'],
                        db_to = dict_infor['db_to'],
                        table_from = dict_infor['table_from'],
                        table_to=dict_infor['table_to'],
                        primary_key = dict_infor['primary_key'])
        
        pyjin.print_logging('mode is {}'.format(mode))
                
        if mode == 'no_table':
            # table이 애초에 존재하지 않을겨우 생성
            # schema structure 가 형성되지 않으므로 추천하지 않음
            print('create_table start')
            create_table.main(acc_from = dict_infor['acc_from'],
                                acc_to = dict_infor['acc_to'],
                                db_from = dict_infor['db_from'],
                                db_to = dict_infor['db_to'],
                                table_from = dict_infor['table_from'],
                                table_to = dict_infor['table_to'],
                                primary_key = dict_infor['primary_key'])
                    
        elif mode is not None:                   
            print('delete_upsert start')
            delete_upsert.Main(acc_from = dict_infor['acc_from'],
                                acc_to = dict_infor['acc_to'],
                                db_from = dict_infor['db_from'],
                                db_to = dict_infor['db_to'],
                                table_from = dict_infor['table_from'],
                                table_to = dict_infor['table_to'],
                                primary_key = dict_infor['primary_key'],
                                mode = mode)()
            
            
                                        
        elif mode is None:
            print('all_delete_insert start')
            all_delete_insert.main(acc_from = dict_infor['acc_from'], 
                                    acc_to = dict_infor['acc_to'],
                                    db_from = dict_infor['db_from'],
                                    db_to = dict_infor['db_to'],
                                    table_from = dict_infor['table_from'],
                                    table_to= dict_infor['table_to'],
                                    col_matching = dict_infor['col_matching'])
                    
        pyjin.print_logging('completed')  
            
    except Exception as e:
        
        pyjin.print_logging("failed, error: {}".format(e))
        print(traceback.format_exc())