import datetime

import pandas as pd

from pyjin import pyjin
from x_x import account_info as ai

def get_query_alter_primary_key(db_name, table_name, primary_key):    
    return "ALTER TABLE {}.{} ADD PRIMARY KEY({})".format(db_name, table_name, primary_key)

def main(acc_from, acc_to, db_from, db_to, table_from, table_to, primary_key):    
    with pyjin.connectDB(**acc_from) as con_from, pyjin.connectDB(**acc_to) as con_to: 
            print('{} started at {}'.format(table_to, datetime.datetime.now()))    
                                 
            # table read from table_from            
            query='''
                select * from {}.{}
            '''.format(db_from, table_from)
            
            print("trying to read {}.{} ".format(db_from, table_from))
            
            try:
                pyjin.execute_query(con_from,"SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
                df=pyjin.execute_query(con_from, query, output='df')
                pyjin.execute_query(con_from,"SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                print('completed'.format(db_from, table_from))
                
            except Exception as e:
                print("failed", e)                
            
            ## table write
            print("trying to write {}.{}...".format(db_to, table_to))
            try:
                with con_to.begin():
                    df.to_sql(table_to, 
                              schema=db_to, 
                              con=con_to, 
                              if_exists='replace', 
                              index=False, 
                              chunksize=1000, 
                              method='multi') ## 큰 query 처리 불가능한 경우를 위해 chunksize 추가
                    
                print('completed')
            except Exception as e:
                print("failed",e)                
            
            # primary key setting            
            if primary_key is not None:                        
                print('trying primary key setting...')
                try:    
                    query_alter_primary_key = get_query_alter_primary_key(db_to, table_to, primary_key)
                    pyjin.execute_query(con_to, query_alter_primary_key)
                    print('completed')
                except Exception as e:
                    print('failed ',e)
            
            print('{} finished at {}'.format(table_to, datetime.datetime.now()))
            
    print('backup completed') 