import pandas as pd
import numpy as np 

from pyjin import pyjin
from module import general

def get_insert_update_delete_ids(df_from, df_to, mode, primary_key):
    ids_from = df_from[primary_key].to_numpy()
    ids_to = df_to[primary_key].to_numpy()

    ## delete ids
    delete_ids = np.setdiff1d(ids_to, ids_from)

    if mode == '{},update_date'.format(primary_key):
        ## update 할 id
        temp = pd.merge(df_from, df_to, on=primary_key)
        update_ids = temp[temp['update_date_x'] != temp['update_date_y']][primary_key].to_numpy()        
    else: ## update 필드가 없는경우
        update_ids = np.array([])

    insert_ids = np.setdiff1d(ids_from, ids_to)
    
    return insert_ids, update_ids, delete_ids
    
def get_upsert_data(acc_from, list_insert_update_ids, columns_df_to, db, table, primary_key):
    with pyjin.connectDB(**acc_from, engine_type='NullPool') as con:         
        pyjin.execute_query(con,"SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
        df_upsert = pyjin.execute_query(con,
                                    """
                                    select {columns} from {db}.{table} where {primary_key} in :ids
                                    """.format(columns= '`'+'`,`'.join(columns_df_to)+'`', 
                                               db=db,
                                               table=table,
                                               primary_key=primary_key), 
                                    ids=list_insert_update_ids,
                                    output='df')
        pyjin.execute_query(con,"SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
    
    return df_upsert

def get_df_whole(acc, mode, db, table):
    with pyjin.connectDB(**acc, engine_type='NullPool') as con:         
        pyjin.execute_query(con,"SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
        df=pyjin.execute_query(con,
                                """
                                select {mode} from {db}.{table}
                                """.format(mode=mode, db=db, table=table)
                                , output='df')  
        
        pyjin.execute_query(con,"SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        return df

def main(acc_from, acc_to, db_from, db_to, table_from, table_to, mode, primary_key, col_matching):    
    ## bring all id, update_date(if eixtst) data from service and anal server
    df_from = get_df_whole(acc=acc_from, mode=mode, db=db_from, table=table_from)    
    df_to = get_df_whole(acc=acc_to, mode=mode, db=db_to, table=table_to)
    
    ## calculate insert_ids, update_ids, delete_ids
    insert_ids, update_ids, delete_ids = get_insert_update_delete_ids(df_from, df_to, mode, primary_key=primary_key)
    
    '''
    update 할것과 delete 할것을 -> delete
    update 할것과 new rows 할것 -> insert
    (update data는 사실상 replaced)
    '''
    list_delete_update_ids = delete_ids.tolist()+ update_ids.tolist()
    list_insert_update_ids = insert_ids.tolist() + update_ids.tolist()    
        
    # upsert
    if len(list_insert_update_ids): 
        columns_bring = general.get_col_matched(
            acc_from = acc_from,
            acc_to = acc_to, 
            db_from = db_from, 
            db_to = db_to, 
            table_from = table_from, 
            table_to = table_to, 
            col_matching= col_matching
        )

        df_upsert = get_upsert_data(acc_from = acc_from, 
                                    list_insert_update_ids= list_insert_update_ids, 
                                    columns_df_to= columns_bring, 
                                    db = db_from,
                                    table = table_from,
                                    primary_key=primary_key)

    ## delete , upsert
    with pyjin.connectDB(**acc_to, engine_type='NullPool') as con:         
        with con.begin():
            # delete_id, update_id 삭제하기
            if len(list_delete_update_ids):
                ## delete upsert_ids
                pyjin.execute_query(con, 
                                        """
                                        delete from {}.{} where {} in :ids
                                        """.format(db_to, table_to, primary_key),
                                            ids = list_delete_update_ids,                                          
                                        is_return=False)            
            pyjin.print_logging('{} (update), {} (delete) rows deleted'.format(len(update_ids), len(delete_ids)))

            # update_id, insert_id 정보 업데이트 하기
            if len(list_insert_update_ids):
                df_upsert.to_sql(table_to, con=con, schema=db_to, index=False, if_exists='append', chunksize=5000, method='multi')

            pyjin.print_logging('{} (update), {} (insert) data inserted'.format(len(update_ids), len(insert_ids)))
    
    return True