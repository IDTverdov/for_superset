# -*- coding: utf-8 -*-
import psycopg2


class DataBase:
    _instance = None
    cnx = None
    host = None
    user = None
    password = None
    database = None

    def __new__(self):
        if not hasattr(self, 'instance'):
            self.instance = super(DataBase, self).__new__(self)
            self.connect(self)
        return self.instance
    
    
    def connect(self):
        try:
            # пытаемся подключиться к базе данных
            self.cnx = psycopg2.connect(dbname='superset',
                                         user='postgres', 
                                         password='пароль, да-да', 
                                         host='localhost')
            self.cnx.set_client_encoding('UTF-8')
        except:
            return 'Can`t establish connection to database'
        
    def query(self, query):
        try:
            cursor = self.cnx.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            cursor.close()
            return result
        except psycopg2.Error as e:
            return e
    
    def query_insert(self, query, params=None):
        try:
            cursor = self.cnx.cursor()
            cursor.execute(query, params)
            self.cnx.commit()
            cursor.close()
        except psycopg2.Error as e:
            return e

    def disconnect(self):
        self.cnx.close()

    def __del__(self):
        try:
            self.disconnect()
        except:
            return 'Error'