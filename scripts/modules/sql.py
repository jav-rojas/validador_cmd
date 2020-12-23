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


class Data(Conexion):

    # Internal Methods
    def parse_kwargs(self, args, query_type=None):
        self.variables = []
        self.values = []

        for key, value in args:
            self.variables.append(key)
            if value == 'NULL':
                self.values.append("{}".format(str(value)))
            else:
                self.values.append("'{}'".format(str(value)))

        if query_type == 'update':
            self.update_variables = [self.variables[i] + " = " + self.values[i] for i in range(len(self.variables))]
            self.update_variables = ', '.join(self.update_variables)
            return self.update_variables
        else:
            self.variables = ', '.join(self.variables)
            self.values = ', '.join(self.values)
            return self.variables, self.values

    # Insert Methods
    def add_data(self, **kwargs):
        self.variables, self.values = self.parse_kwargs(kwargs.items())

        self.create_connection()
        self.c.execute(
            'INSERT INTO IdentifyingData ({}) '
            'VALUES ({})'.format(self.variables, self.values))
        self.commit_close_connection()

    # Update Methods
    def update(self, identifier, identifier_value, **kwargs):
        self.update_variables = self.parse_kwargs(kwargs.items(), query_type='update')

        self.create_connection()
        self.c.execute(
            'UPDATE IdentifyingData '
            'SET {} WHERE {} = "{}"'.format(self.update_variables, identifier, identifier_value))
        self.commit_close_connection()

    # Select Methods
    def search(self, **kwargs):
        self.variables, self.values = self.parse_kwargs(kwargs.items())

        self.create_connection()
        self.c.execute('SELECT * FROM IdentifyingData WHERE {} = {}'.format(self.variables, self.values))
        self.data = self.c.fetchall()

        if self.data:
            return self.data
        else:
            return False


class Giftcards(Conexion):

    # Internal Methods
    def parse_kwargs(self, args, query_type=None):
        self.variables = []
        self.values = []

        for key, value in args:
            self.variables.append(key)
            if value == 'NULL':
                self.values.append("{}".format(str(value)))
            else:
                self.values.append("'{}'".format(str(value)))

        if query_type == 'update':
            self.update_variables = [self.variables[i] + " = " + self.values[i] for i in range(len(self.variables))]
            self.update_variables = ', '.join(self.update_variables)
            return self.update_variables
        else:
            self.variables = ', '.join(self.variables)
            self.values = ', '.join(self.values)
            return self.variables, self.values

    # Insert Methods
    def add_data(self, **kwargs):
        self.variables, self.values = self.parse_kwargs(kwargs.items())

        self.create_connection()
        self.c.execute(
            'INSERT INTO Giftcards ({}) '
            'VALUES ({})'.format(self.variables, self.values))
        self.commit_close_connection()

    # Update Methods
    def update(self, identifier, identifier_value, **kwargs):
        self.update_variables = self.parse_kwargs(kwargs.items(), query_type='update')

        self.create_connection()
        self.c.execute(
            'UPDATE Giftcards '
            'SET {} WHERE {} = "{}"'.format(self.update_variables, identifier, identifier_value))
        self.commit_close_connection()
    
    # Select Methods
    def select(self, *args, identifier=False, identifier_value=False, gidentifier=False, gidentifier_value=False, random=False):
        self.variables = list(args)
        self.variables = ', '.join(self.variables)

        self.tv_identifier = identifier and identifier_value
        self.tv_gidentifier = gidentifier and gidentifier_value

        if self.tv_identifier and not self.tv_gidentifier and not random:
            if self.variables == '':
                self.variables = '*'
            self.create_connection()
            self.c.execute(
                'SELECT {} FROM Giftcards WHERE {} = "{}"'.format(self.variables, identifier, identifier_value))
            self.data = self.c.fetchall()

            if self.data:
                return self.data
            else:
                return False
            self.commit_close_connection()

        elif self.tv_identifier and self.tv_gidentifier and not random:
            if self.variables == '':
                self.variables = '*'
            self.create_connection()
            self.c.execute(
                'SELECT {} FROM Giftcards WHERE {} = "{}" AND {} = "{}"'.format(self.variables, identifier, identifier_value, gidentifier, gidentifier_value))
            self.data = self.c.fetchall()

            if self.data:
                return self.data
            else:
                return False
            self.commit_close_connection()

        elif self.tv_identifier and self.tv_gidentifier and random:
            if self.variables == '':
                self.variables = '*'
            self.create_connection()
            self.c.execute(
                'SELECT {} FROM Giftcards WHERE {} IS NULL AND {} = "{}" ORDER BY RAND() LIMIT 1'.format(self.variables, identifier, gidentifier, gidentifier_value))
            self.data = self.c.fetchall()

            if self.data:
                return self.data
            else:
                return False
            self.commit_close_connection()

        elif self.tv_identifier and not self.tv_gidentifier and random:
            if self.variables == '':
                self.variables = '*'
            self.create_connection()
            self.c.execute(
                'SELECT {} FROM Giftcards WHERE {} IS NULL ORDER BY RAND() LIMIT 1'.format(self.variables, identifier))
            self.data = self.c.fetchall()

            if self.data:
                return self.data
            else:
                return False
            self.commit_close_connection()

        else:
            self.create_connection()
            self.c.execute(
                'SELECT * FROM Giftcards')
            self.data = self.c.fetchall()

            if self.data:
                return self.data
            else:
                return False
            self.commit_close_connection()


class GiftcardsPins(Conexion):

    # Internal Methods
    def parse_kwargs(self, args, query_type=None):
        self.variables = []
        self.values = []

        for key, value in args:
            self.variables.append(key)
            if value == 'NULL':
                self.values.append("{}".format(str(value)))
            else:
                self.values.append("'{}'".format(str(value)))

        if query_type == 'update':
            self.update_variables = [self.variables[i] + " = " + self.values[i] for i in range(len(self.variables))]
            self.update_variables = ', '.join(self.update_variables)
            return self.update_variables
        else:
            self.variables = ', '.join(self.variables)
            self.values = ', '.join(self.values)
            return self.variables, self.values

    # Insert Methods
    def add_data(self, **kwargs):
        self.variables, self.values = self.parse_kwargs(kwargs.items())

        self.create_connection()
        self.c.execute(
            'INSERT INTO GiftcardsPins ({}) '
            'VALUES ({})'.format(self.variables, self.values))
        self.commit_close_connection()

    # Update Methods
    def update(self, identifier, identifier_value, **kwargs):
        self.update_variables = self.parse_kwargs(kwargs.items(), query_type='update')

        self.create_connection()
        self.c.execute(
            'UPDATE GiftcardsPins '
            'SET {} WHERE {} = "{}"'.format(self.update_variables, identifier, identifier_value))
        self.commit_close_connection()
    
    # Select Methods
    def select(self, *args, identifier=False, identifier_value=False, random=False):
        self.variables = list(args)
        self.variables = ', '.join(self.variables)

        if identifier and identifier_value and not random:
            if self.variables == '':
                self.variables = '*'
            self.create_connection()
            self.c.execute(
                'SELECT {} FROM GiftcardsPins WHERE {} = "{}"'.format(self.variables, identifier, identifier_value))
            self.data = self.c.fetchall()

            if self.data:
                return self.data
            else:
                return False
            self.commit_close_connection()

        elif identifier and identifier_value and random:
            if self.variables == '':
                self.variables = '*'
            self.create_connection()
            self.c.execute(
                'SELECT {} FROM GiftcardsPins WHERE {} IS NULL ORDER BY RAND() LIMIT 1'.format(self.variables, identifier, identifier_value))
            self.data = self.c.fetchall()

            if self.data:
                return self.data
            else:
                return False
            self.commit_close_connection()

        else:
            self.create_connection()
            self.c.execute(
                'SELECT * FROM GiftcardsPins')
            self.data = self.c.fetchall()

            if self.data:
                return self.data
            else:
                return False
            self.commit_close_connection()

class SmsData(Conexion):

    # Internal Methods
    def parse_kwargs(self, args, query_type=None):
        self.variables = []
        self.values = []

        for key, value in args:
            self.variables.append(key)
            if value == 'NULL':
                self.values.append("{}".format(str(value)))
            else:
                self.values.append("'{}'".format(str(value)))

        if query_type == 'update':
            self.update_variables = [self.variables[i] + " = " + self.values[i] for i in range(len(self.variables))]
            self.update_variables = ', '.join(self.update_variables)
            return self.update_variables
        else:
            self.variables = ', '.join(self.variables)
            self.values = ', '.join(self.values)
            return self.variables, self.values

    # Insert Methods
    def add_data(self, **kwargs):
        self.variables, self.values = self.parse_kwargs(kwargs.items())

        self.create_connection()
        self.c.execute(
            'INSERT INTO SmsData ({}) '
            'VALUES ({})'.format(self.variables, self.values))
        self.commit_close_connection()

    # Update Methods
    def update(self, identifier, identifier_value, **kwargs):
        self.update_variables = self.parse_kwargs(kwargs.items(), query_type='update')

        self.create_connection()
        self.c.execute(
            'UPDATE SmsData '
            'SET {} WHERE {} = "{}"'.format(self.update_variables, identifier, identifier_value))
        self.commit_close_connection()
    
    # Select Methods
    def select(self, *args, identifier=False, identifier_value=False, random=False):
        self.variables = list(args)
        self.variables = ', '.join(self.variables)

        if identifier and identifier_value and not random:
            if self.variables == '':
                self.variables = '*'
            self.create_connection()
            self.c.execute(
                'SELECT {} FROM SmsData WHERE {} = "{}"'.format(self.variables, identifier, identifier_value))
            self.data = self.c.fetchall()

            if self.data:
                return self.data
            else:
                return False
            self.commit_close_connection()

        elif identifier and identifier_value and random:
            if self.variables == '':
                self.variables = '*'
            self.create_connection()
            self.c.execute(
                'SELECT {} FROM SmsData WHERE {} IS NULL ORDER BY RAND() LIMIT 1'.format(self.variables, identifier, identifier_value))
            self.data = self.c.fetchall()

            if self.data:
                return self.data
            else:
                return False
            self.commit_close_connection()

        else:
            self.create_connection()
            self.c.execute(
                'SELECT * FROM SmsData')
            self.data = self.c.fetchall()

            if self.data:
                return self.data
            else:
                return False
            self.commit_close_connection()

class EmailData(Conexion):

    # Internal Methods
    def parse_kwargs(self, args, query_type=None):
        self.variables = []
        self.values = []

        for key, value in args:
            self.variables.append(key)
            if value == 'NULL':
                self.values.append("{}".format(str(value)))
            else:
                self.values.append("'{}'".format(str(value)))

        if query_type == 'update':
            self.update_variables = [self.variables[i] + " = " + self.values[i] for i in range(len(self.variables))]
            self.update_variables = ', '.join(self.update_variables)
            return self.update_variables
        else:
            self.variables = ', '.join(self.variables)
            self.values = ', '.join(self.values)
            return self.variables, self.values

    # Insert Methods
    def add_data(self, **kwargs):
        self.variables, self.values = self.parse_kwargs(kwargs.items())

        self.create_connection()
        self.c.execute(
            'INSERT INTO EmailData ({}) '
            'VALUES ({})'.format(self.variables, self.values))
        self.commit_close_connection()

    # Update Methods
    def update(self, identifier, identifier_value, **kwargs):
        self.update_variables = self.parse_kwargs(kwargs.items(), query_type='update')

        self.create_connection()
        self.c.execute(
            'UPDATE EmailData '
            'SET {} WHERE {} = "{}"'.format(self.update_variables, identifier, identifier_value))
        self.commit_close_connection()
    
    # Select Methods
    def select(self, *args, identifier=False, identifier_value=False, random=False):
        self.variables = list(args)
        self.variables = ', '.join(self.variables)

        if identifier and identifier_value and not random:
            if self.variables == '':
                self.variables = '*'
            self.create_connection()
            self.c.execute(
                'SELECT {} FROM EmailData WHERE {} = "{}"'.format(self.variables, identifier, identifier_value))
            self.data = self.c.fetchall()

            if self.data:
                return self.data
            else:
                return False
            self.commit_close_connection()

        elif identifier and identifier_value and random:
            if self.variables == '':
                self.variables = '*'
            self.create_connection()
            self.c.execute(
                'SELECT {} FROM EmailData WHERE {} IS NULL ORDER BY RAND() LIMIT 1'.format(self.variables, identifier, identifier_value))
            self.data = self.c.fetchall()

            if self.data:
                return self.data
            else:
                return False
            self.commit_close_connection()

        else:
            self.create_connection()
            self.c.execute(
                'SELECT * FROM EmailData')
            self.data = self.c.fetchall()

            if self.data:
                return self.data
            else:
                return False
            self.commit_close_connection()

class Asignaciones(Conexion):

    # Internal Methods
    def parse_kwargs(self, args, query_type=None):
        self.variables = []
        self.values = []

        for key, value in args:
            self.variables.append(key)
            if value == 'NULL':
                self.values.append("{}".format(str(value)))
            else:
                self.values.append("'{}'".format(str(value)))

        if query_type == 'update':
            self.update_variables = [self.variables[i] + " = " + self.values[i] for i in range(len(self.variables))]
            self.update_variables = ', '.join(self.update_variables)
            return self.update_variables
        else:
            self.variables = ', '.join(self.variables)
            self.values = ', '.join(self.values)
            return self.variables, self.values

    # Insert Methods
    def add_data(self, **kwargs):
        self.variables, self.values = self.parse_kwargs(kwargs.items())

        self.create_connection()
        self.c.execute(
            'INSERT INTO Asignaciones ({}) '
            'VALUES ({})'.format(self.variables, self.values))
        self.commit_close_connection()

    # Update Methods
    def update(self, identifier, identifier_value, **kwargs):
        self.update_variables = self.parse_kwargs(kwargs.items(), query_type='update')

        self.create_connection()
        self.c.execute(
            'UPDATE Asignaciones '
            'SET {} WHERE {} = "{}"'.format(self.update_variables, identifier, identifier_value))
        self.commit_close_connection()
    
    # Select Methods
    def select(self, *args, identifier=False, identifier_value=False, random=False):
        self.variables = list(args)
        self.variables = ', '.join(self.variables)

        if identifier and identifier_value and not random:
            if self.variables == '':
                self.variables = '*'
            self.create_connection()
            self.c.execute(
                'SELECT {} FROM Asignaciones WHERE {} = "{}"'.format(self.variables, identifier, identifier_value))
            self.data = self.c.fetchall()

            if self.data:
                return self.data
            else:
                return False
            self.commit_close_connection()

        elif identifier and identifier_value and random:
            if self.variables == '':
                self.variables = '*'
            self.create_connection()
            self.c.execute(
                'SELECT {} FROM Asignaciones WHERE {} IS NULL ORDER BY RAND() LIMIT 1'.format(self.variables, identifier, identifier_value))
            self.data = self.c.fetchall()

            if self.data:
                return self.data
            else:
                return False
            self.commit_close_connection()

        else:
            self.create_connection()
            self.c.execute(
                'SELECT * FROM Asignaciones')
            self.data = self.c.fetchall()

            if self.data:
                return self.data
            else:
                return False
            self.commit_close_connection()