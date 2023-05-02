import sqlite3
from os import path
# class for connecting to a sqlite database
# the benefit is that we can create methods for interacting (fetch, execute, names) that use the connect
# and disconnect methods to open and close the connection to the db instead of needing to either leave an open
# connection or open and close constantly when accessing the db


class DbConnect:
    # init takes path of the .db as argument
    def __init__(self, db_path, create=False):
        if not path.exists(db_path) and not create:
            print(f"{db_path} does not exist. Please choose an existing database or set create = True when initiating "
                  "builder.")
            raise FileNotFoundError
        else:
            self.dbPath = db_path

    def __connect__(self):
        try:
            self.con = sqlite3.connect(self.dbPath)             # open connection to .db at dbPath
            self.con.text_factory = lambda x: str(x, 'latin1')  # encodes the text as latin1
            self.cur = self.con.cursor()                        # initiates a cursor
            return True
        except sqlite3.Error as error:
            print("Error while connecting to sqlite: ", error)
            return False

    def __disconnect__(self):
        try:
            self.con.close()                # close the connection
            return True
        except sqlite3.Error as error:
            print("Didn't close: ", error)
            return False

    def fetch(self, sql):                   # method for fetching data takes sql statement as argument
        connected = self.__connect__()                  # open the connection
        if connected:
            try:
                self.cur.execute(sql)               # execute the sql statement
                result = self.cur.fetchall()        # fetchall and store in result
            except sqlite3.Error as error:
                print("Failed to fetch Data: ", error)
                print(sql)
                return False
            self.__disconnect__()               # close the connection
            return result                       # return data
        else:
            return False
    
    def execute(self, sql):                 # method for executing sql operations to the database
        connected = self.__connect__()                  # open the connection
        if connected:
            try:
                self.cur.execute(sql)               # execute the sql command
                self.con.commit()                   # commit the changes to the db (save)
            except sqlite3.Error as error:
                print("Failed to execute command: ", error)
                return False
            self.__disconnect__()               # close the connection
            return True
        else:
            return False

    def names(self, tableName):
        connected = self.__connect__()                  # open the connection
        if connected:
            sql = f'''SELECT * FROM {tableName}'''  # SQL selects everything from the table
            try:
                data = self.cur.execute(sql)        # gets the data
            except sqlite3.Error as error:
                print("Failed to get field names from table: ", error)
                return False
            self.__disconnect__()               # close the connection
            return_data = []
            for dat in data.description:
                for thing in dat:
                    if thing:
                        return_data.append(thing)
            return tuple(return_data)             # return names in a format we will map in CoreLogic
        else:
            return False

    def tables(self):                       # method to get names of tables in a db
        connected = self.__connect__()                  # open the connection
        if connected:
            sql = "SELECT name FROM sqlite_master WHERE type='table';"
            try:
                tables = self.fetch(sql)        # call the fetch method on the sql statement
            except sqlite3.Error as error:
                print('Failed to get table names from database: ', error)
                return False
            self.__disconnect__()               # close the connection
            table_names = []
            for table in tables:
                if table[0] != 'sqlite_sequence':
                    table_names.append(table[0])
            return table_names                   # return the list of table names
        else:
            return False


def compare(tbl1, t1_db_dict, tbl2, t2_db_dict, columns):
    if not isinstance(columns, str):
        columns = ', '.join(columns)
    if t1_db_dict == t2_db_dict:
        conn = DbConnect(t1_db_dict['file_path'])
        sql = f"SELECT {columns} FROM {tbl1} EXCEPT SELECT {columns} FROM {tbl2}"
        results = conn.fetch(sql)
    else:
        try:
            conn = sqlite3.connect(t1_db_dict['file_path'])
            conn.text_factory = lambda x: str(x, 'latin1')
            cur = conn.cursor()
        except sqlite3.Error as error:
            print("Error while connecting to sqlite: ", error)
            return False
        sql = f"ATTACH DATABASE '{t2_db_dict['file_path']}' AS db2;"
        try:
            cur.execute(sql)  # execute the sql command
            conn.commit()  # commit the changes to the db (save)
        except sqlite3.Error as error:
            print("Failed to execute command: ", error)
            return False
        sql = f"SELECT {columns} FROM {tbl1} EXCEPT SELECT {columns} FROM db2.{tbl2}"
        try:
            cur.execute(sql)
            results = cur.fetchall()
        except sqlite3.Error as error:
            print("Failed to execute command: ", error)
            return False
        try:
            conn.close()                # close the connection
        except sqlite3.Error as error:
            print("Didn't close: ", error)
            return False
    if results:
        return False
    else:
        return True
