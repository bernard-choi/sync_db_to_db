# coding: utf-8
import datetime

import pandas as pd
import numpy as np

from pyjin import pyjin
from module import general

def main(acc_from, 
         acc_to, 
         db_from, 
         db_to, 
         table_from, 
         table_to,
         col_matching):
    
    '''
    service server 에서 데이터 가져오기
    '''
    with pyjin.connectDB(**acc_from, engine_type='NullPool') as con:
        query='''
        select * from {}.{}
        '''.format(db_from, table_from)
        pyjin.execute_query(con,"SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
        df_from=pyjin.execute_query(con, query, output='df')
        pyjin.execute_query(con,"SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")        
        
                
    columns_bring = general.get_col_matched(
        acc_from = acc_from,
        acc_to = acc_to, 
        db_from = db_from, 
        db_to = db_to, 
        table_from = table_from, 
        table_to = table_to, 
        col_matching= col_matching    
        )

    df_from = df_from[columns_bring] 

    '''
    delete all and insert all anal server db table
    ''' 
    with pyjin.connectDB(**acc_to, engine_type='NullPool') as con:            
        try:                
            ## transaction
            with con.begin():
                ## delete all data
                pyjin.execute_query(con, 'delete from {}.{}'.format(db_to, table_to))                
                ## write data
                df_from.to_sql(table_to, con=con, if_exists='append', index=False, chunksize=5000, method='multi', schema=db_to) ## 큰 query 처리 불가능한 경우를 위해 chunksize 추가
        
        except Exception as e:
            pyjin.print_logging("{} delete and write failed, {}".format(table_to, e))

    pyjin.print_logging('{} completed'.format(table_to))

    return True