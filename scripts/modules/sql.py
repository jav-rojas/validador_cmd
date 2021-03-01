import mysql.connector
import numpy as np
import pandas as pd
from .config import credentials

credentials = credentials()
host = credentials.host
user = credentials.text_user
password = credentials.pass_sql
database = credentials.database


class Conexion():

    # Attributes (por ahora ninguno, irán en constructor)

    # Constructor
    def __init__(self, host=host, user=user, passwd=password, database=database):
        self.host = host
        self.user = user
        self.passwd = passwd
        self.database = database

    # Methods
    def create_connection(self):
        self.db_connection = mysql.connector.connect(
            host=self.host,
            user=self.user,
            passwd=self.passwd,
            database=self.database)
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


class Usuarios(Conexion):

    # Internal methods
    def parse_kwargs(self, args):
        self.variables = []
        self.values = []

        for key, value in args:
            self.variables.append(key)
            self.values.append("'{}'".format(str(value)))

        self.variables = ', '.join(self.variables)
        self.values = ', '.join(self.values)

        return self.variables, self.values

    # Methods
    def add_user(self, **kwargs):
        self.variables, self.values = self.parse_kwargs(kwargs.items())
        self.create_connection()
        self.c.execute('INSERT INTO Username ({}) VALUES ({})'.format(self.variables, self.values))
        self.commit_close_connection()

    def update_last(self, username, updated_at):
        self.create_connection()
        self.c.execute('UPDATE Username SET updated_at = "{}" WHERE username = "{}"'.format(updated_at, username))
        self.commit_close_connection()

    def login_user(self, username, password):
        # En cada login, recupera solo si el usuario y la contraseña coinciden:
        self.create_connection()
        self.c.execute('SELECT * FROM Username WHERE username = "{}" AND password = "{}"'.format(username, password))
        self.data = self.c.fetchall()
        self.commit_close_connection()
        return self.data

    def view_all_users_logininfo(self):
        self.create_connection()
        self.c.execute('SELECT username, password FROM Username')
        self.data = self.c.fetchall()
        self.commit_close_connection()
        return self.data

    def view_all_users_info(self):
        self.create_connection()
        self.c.execute('SELECT username, first_name, last_name, email, created_at, updated_at FROM Username')
        self.data = self.c.fetchall()
        self.commit_close_connection()
        return self.data