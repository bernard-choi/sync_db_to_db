from pyjin import pyjin

def get_col_matched(acc_from, acc_to, db_from, db_to, table_from, table_to, col_matching):
    ## bring all columns of the table_to
    columns_df_to = pyjin.conn_exec_close(acc_to, 
                                    """
                                    select column_name
                                    from INFORMATION_SCHEMA.COLUMNS
                                    where TABLE_NAME=:table and TABLE_SCHEMA =:db
                                    """, db=db_to, table=table_to, output='df')['column_name'].tolist()  
    
    columns_df_from = pyjin.conn_exec_close(acc_from, 
                                """
                                select column_name
                                from INFORMATION_SCHEMA.COLUMNS
                                where TABLE_NAME=:table and TABLE_SCHEMA =:db
                                """, db=db_from, table=table_from, output='df')['column_name'].tolist() 
    
    if col_matching == 'both':
        columns_bring = list(set(columns_df_from).intersection(set(columns_df_to)))
    elif col_matching == 'to':
        columns_bring = columns_df_to
    elif col_matching == 'from':
        columns_bring= columns_df_from
    else:
        raise Exception('not proper matching_mode')

    return columns_bring