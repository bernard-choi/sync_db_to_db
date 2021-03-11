def analdb_sql_info(db='ople'):
    return {
        'host' :'datatest.koreacentral.cloudapp.azure.com',
        'user' : 'ople',
        'password' : 'x_x@song2ro',
        'db' : db,
        'port' : 5000,
    }

def servicedb_sql_info(**kwars):
    return {
        'host' :'ople-db.koreacentral.cloudapp.azure.com',
        'user' : 'ople',
        'password' : 'ohmysql@x_x.co.kr',
        'db' : 'ople',
        'port' : 5000,
    }

def vmdb_sql_info(db='ople'):
    return {
        'host' :'localhost',
        'user' : 'ople',
        'password' : 'x_x@song2ro',
        'db' : db,
        'port' : 3306,
    }

# dbtype default : mysql
def vmdb2_sql_info(db='postgres'):
    return {
        'host' :'localhost',
        'user' : 'postgres',
        'password' : 'x_x@song2ro',
        'db' : db,
        'port' : 5432,
        'dbtype' : 'postgresql+psycopg2'
    }