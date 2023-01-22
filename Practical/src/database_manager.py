import psycopg2
import datetime
import json

"""
    @Notice: This function will return the json in a file
    @Dev:   We open the file at the file path and return it as a json using json.load
"""
def get_dict_file(file_path: str):
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)

class DataBase_Manager:
    def __init__(self):
        self.database_info = get_dict_file("./files/database_params.json")
        self.env = "TEST"

    """
     @Notice: This function will be used connect to the postgre sql database
     @Dev:   We use the psycopg2 lib to connect to the database
    """
    def connect(self):
        try:
            self.connection = psycopg2.connect("dbname='{dbname}' user='{user}' host='{host}' password='{password}' port='{port}'".format(
                dbname=self.database_info[self.env]["db_name"],
                user=self.database_info[self.env]["user"],
                password=self.database_info[self.env]["password"],
                host=self.database_info[self.env]["host"],
                port=self.database_info[self.env]["port"]))
            self.cursor = self.connection.cursor()
        except Exception as e:
            raise e

    """
     @Notice: This function will be used disconnect to the postgre sql database
     @Dev:   We first verify if the database is connected, if yes we use the psycopg2 lib to disconnect to the database
    """
    def disconnect(self):
        if self.connection:
            self.cursor.close()
            self.connection.close()
        
    def execute(self, query, item_tupple):
        try:
            self.connect()
            self.cursor.execute(query, item_tupple)
            self.connection.commit()
            self.disconnect()
        except Exception as e:
            raise e
    
    def select_all(self, query):
        try:
            self.connect()
            self.cursor.execute(query)
            items = self.cursor.fetchall()
            self.disconnect()
            return items
        except Exception as e:
            raise e