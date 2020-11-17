import pandas as pd
import config
from pymysql import connect, Error

credentials = config.credentials()
password = credentials.pass_sql
user = credentials.text_user

class conn_sql():

    # Constructor
    def __init__(self, host='162.243.165.69', port=3306, user=user, passwd=password, db=None):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.db = db

    # Methods
    def create_connection(self):
        self.db_connection = connect(
            host=self.host,
            port=self.port,
            user=self.user,
            passwd=self.passwd,
            database=self.db)
        self.c = self.db_connection.cursor()

    def commit_close_connection(self):
        self.db_connection.commit()
        self.c.close()
        self.db_connection.close()

    def execute(self, query):
        self.create_connection()
        self.c.execute(query)
        self.data = self.c.fetchall()
        return self.data


class dl_sql(conn_sql):

    data = None
    result = None

    def descarga(self, query=None, duplicates_level=None):
        try:
            self.create_connection()
            self.data = pd.read_sql(query, con=self.db_connection)

            if duplicates_level == 'interview__key':
                self.data = self.data.sort_values(by=['act']).drop_duplicates(subset=['interview__key'], keep='last')
            if duplicates_level == 'orden':
                self.data = self.data.sort_values(by=['act']).drop_duplicates(subset=['interview__key', 'orden'], keep='last')

            self.result = 'Completado'
            self.commit_close_connection()
            return self.data, self.result
        except Exception as e:
            self.result = e
            return self.data, self.result


class keys_parser():

    def parse(self, keys, query=False, query_type=None, args_sql=[None, None, None]):
        keys_list = keys.splitlines()
        for i in range(0, len(keys_list)):
            str_val = '{}'.format(keys_list[i])
            keys_list[i] = str_val
        if query:
            if query_type == 'select':
                str_keys = str(tuple(keys_list))
                query = "SELECT * FROM {}.{} WHERE interview__key in {}".format(args_sql[0], args_sql[0], str_keys)
                return query
            if query_type == 'update':
                str_keys = str(tuple(keys_list))
                query = "UPDATE {}.{} SET {} = {} WHERE interview__key in {}".format(args_sql[0], args_sql[0], args_sql[1], args_sql[2], str_keys)
                return query
        else:
            return keys_list
