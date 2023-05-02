from SQL_Builder import db_ops as conn
from os.path import abspath


def where_builder(where, case_sens, where_and):
    if not where_and:
        where_and = "OR"
    else:
        where_and = "AND"
    sql = " WHERE "
    if type(where) == dict:
        if where_and == 'OR':
            print("Note: If your results are not as full as you expect, it may be because you used a dictionary. This "
                  "is allowed but can cause problems as there can only be one value per key and this can mean your "
                  "values are being replaced. Use a list or tuple of tuple pairs instead with OR.")
        for entry in list(where.keys())[:-1]:
            if not case_sens:
                sql += f"LOWER({entry})='{where[entry].lower()}' {where_and} "
            else:
                sql += f"{entry}='{where[entry]}' {where_and} "
        if not case_sens:
            sql += f"LOWER({list(where.keys())[-1]})='{where[list(where.keys())[-1]].lower()}' "
        else:
            sql += f"{list(where.keys())[-1]}='{where[list(where.keys())[-1]]}' "
    elif type(where) == tuple or type(where) == list:
        where = tuple(where)
        where2 = tuple([x for x in where if (type(x) == list or type(x) == tuple) and len(x) == 2])
        if where != where2:
            print("Ignored one or more entries in the where= argument because they were incorrect. Each entry must be "
                  "a list or tuple of length 2. Index 0 is field name being searched and index 1 is value to be "
                  "matched.")
        for pair in where2[:-1]:
            if not case_sens:
                sql += f"LOWER({pair[0]})='{pair[1].lower()}' {where_and} "
            else:
                sql += f"{pair[0]}='{pair[1]}' {where_and} "
        if not case_sens:
            sql += f"LOWER({where2[-1][0]})='{where2[-1][1].lower()}'"
        else:
            sql += f"{where2[-1][0]}='{where2[-1][1]}'"
    elif type(where) == str:
        sql += where
    else:
        print('where= must be a dictionary, list, tuple, or string. If it is a list or a tuple, the contents must be '
              'lists or tuples containing pairs where index[0] is the field name and index[1] is the value being '
              'matched.')
    return sql


def data_builder(data, sql, act):
    if isinstance(data, str):
        return f"{data} TEXT;"
    if isinstance(data, list) or isinstance(data, tuple):
        data = {key: {'type': 'text'} for key in data}
    if not isinstance(data, dict):
        print("The data passed into add or rename column must be a string (one column), a list (for multiple columns "
              "where each is type Text, no default, and can be null), or a dictionary where each key is a column name "
              "and each key's value is a dictionary with at least type, but can include 'not_null' and 'default' i.e.: "
              "{'column_name': {'type': 'text, integer, etc.', 'default': 'default_text', 'not_null': 'True for "
              "cannot be null, False or not present can be null.'}}")
    for i, (key, val) in enumerate(data.items()):
        if not isinstance(val, dict):
            print("Double check that the value for each key in the dictionary is a dictionary consisting of that "
                  "column's information (type, default, not_null, and unique)")
            exit()

        sql += f"{key}"
        col_type = data[key].get('type')
        if col_type:
            sql += f" {col_type.upper()}"
        else:
            sql += ' TEXT'
        default = data[key].get('default')
        if default:
            sql += f" DEFAULT '{data[key]['default']}'"
        not_null = data[key].get('not_null')
        if not_null:
            sql += ' NOT NULL'
        if act == 'create':
            unique = data[key].get('unique')
            if unique:
                sql += " UNIQUE"
            sql += ", "
        elif act == 'add':
            sql += '; '
    return sql


class Build:
    def __init__(self, db_info='', create=False):
        self.allowed_params = ['alias', 'backup', 'default']
        if db_info == '':
            print("Something went wrong. It shouldn't have let you get here with no db_info passed in.")
            exit()
        db_info = self.dict_checker(db_info, db_info=True)
        for key in db_info:
            db_info[key]['instance'] = conn.DbConnect(db_info[key]['file_path'], create=create)
            db_info[key]['tables'] = db_info[key]['instance'].tables()
        self.conn_dict = db_info
        for key in self.conn_dict:
            if self.conn_dict[key]['default']:
                self.default_db_info = self.conn_dict[key]
            if self.conn_dict[key]['backup']:
                backup = self.conn_dict[key]['backup']
                main_tables = [x.lower() for x in self.conn_dict[key]['tables']]
                backup_tables = [x.lower() for x in self.conn_dict[backup]['tables']]
                if main_tables != backup_tables:
                    print(f"The tables in {key} do not correspond to the tables in {backup}. {backup} was listed as "
                          f"the backup of {key}. This feature is so that you don't overwrite good info by providing "
                          f"incorrect backup information.")
                    exit()
                if main_tables:
                    for table in main_tables:
                        main_columns = [x.lower() for x in self.conn_dict[key]['instance'].names(table)]
                else:
                    main_columns = []
                if backup_tables:
                    backup_columns = [x.lower() for x in self.conn_dict[backup]['instance'].names(table)]
                else:
                    backup_columns = []
                if main_columns != backup_columns:
                    print(f"The column names in table {table} in the database {key} do not match the column names in "
                          f"the same table in {backup}. Because {backup} was listed as the backup for {key}, "
                          f"the two must have the same tables with the same columns. This is to prevent data loss.")
                    exit()

    def column_names(self, db_alias='default', table='default'):
        if db_alias.lower() == 'default':
            db_alias = self.default_db_info['alias']
        if table.lower() == 'default' and self.conn_dict[db_alias]['tables'] and len(self.conn_dict[db_alias]['tables']) == 1:
            table = self.conn_dict[db_alias]['tables'][0]
        elif table.lower() == 'default' and self.conn_dict[db_alias]['tables'] and len(self.conn_dict[db_alias]['tables']) > 1:
            print(f"The database at alias {db_alias} has more than one table. Leaving table blank is only possible for "
                  f"databases that have only one table.")
            exit()
        try:
            column_names = self.conn_dict[db_alias]['instance'].names(table)
        except KeyError:
            print("That is not one of the db aliases you initiated")
            return False
        return column_names

    def table_names(self, db_alias='default'):
        table = None
        if not isinstance(db_alias, str) and not isinstance(db_alias, int):
            print("You can only return table names for one database at a time. Alias must be a string or integer")
            exit()
        if db_alias not in self.conn_dict.keys():
            print("That is not one of the db aliases you initiated")
            return False

        table_names = self.conn_dict[db_alias]['instance'].tables()
        return table_names

    def select(self, db_alias='default', table='', columns='*', where='', where_and=True, case_sens=True):
        if table == '':
            print("You must enter a table name to select rows")
            exit()
        if not isinstance(db_alias, str):
            print("You can only select rows from one database at a time.")
            exit()
        if isinstance(db_alias, str) and db_alias.lower() == 'default':
            db_alias = self.default_db_info['alias']
        if isinstance(columns, str) and columns.lower() == 'all':
            columns = '*'
        if type(columns) == str:
            sql = f"SELECT {columns} FROM {table}"
        else:
            sql = f"SELECT {', '.join(columns)} FROM {table}"

        if where != '':
            sql += where_builder(where, case_sens, where_and)

        if type(db_alias) == list or isinstance(db_alias, tuple):
            print("You can't use a list or tuple for fetch, it can only be used for methods that make changes to a db")
            return False
        try:
            results = self.conn_dict[db_alias]['instance'].fetch(sql)
        except KeyError:
            print("That is not one of the db aliases you initiated")
            return False

        return results

    def insert(self, db_alias='default', table='', data=''):
        if isinstance(db_alias, str) and db_alias.lower() == 'default':
            db_alias = self.default_db_info['alias']
        if table == '':
            print('You must select a table to insert rows.')
            exit()
        data = self.dict_checker(data)

        sql = f"INSERT INTO {table}("
        sql2 = f" VALUES("
        for item in list(data.keys())[:-1]:
            sql += f"{item}, "
            sql2 += f"'{data[item]}', "
        sql += f"{list(data.keys())[-1]})"
        sql2 += f"'{data[list(data.keys())[-1]]}')"
        sql += sql2
        executed = self.check_commit(db_alias, sql)
        if not executed:
            print(f"SQL insert failed: {sql}")
            return False
        return True

    def update(self, db_alias='default', table='', data='', where='', where_and=True, case_sens=True):
        if isinstance(db_alias, str) and db_alias.lower() == 'default':
            db_alias = self.default_db_info['alias']
        if table == '':
            print('You must select a table to insert rows.')
            exit()
        data = self.dict_checker(data)

        sql = f"UPDATE {table} SET "
        for item in list(data.keys())[:-1]:
            sql += f"{item}='{data[item]}', "
        sql += f"{list(data.keys())[-1]}='{data[list(data.keys())[-1]]}'"

        if where != '':
            sql += where_builder(where, case_sens, where_and)

        executed = self.check_commit(db_alias, sql)
        if not executed:
            print(f"SQL update failed: {sql}")
            return False
        return True

    def create_table(self, db_alias='default', table='', column_data='', primary_key='db_id'):
        # EVENTUALLY MAY WANT TO ADD OTHER TYPES, BUT SQLITE3 ONLY REALLY USES TEXT
        # THIS ALLOWS FOR FREEFORM, BUT IT DOESN'T CORRECT YOU IF YOU TYPE IN A WRONG TYPE NAME
        if table == '':
            print("You must enter a table name to create a table.")
            return False
        if isinstance(db_alias, str) and db_alias.lower() == 'default':
            db_alias = self.default_db_info['alias']
        if type(table) != str:
            print("You can only create one table at a time")
            return False
        sql = f"CREATE TABLE {table}({primary_key} INTEGER NOT NULL UNIQUE, "

        column_dict = {}
        if isinstance(column_data, list) or isinstance(column_data, tuple):
            for thing in column_data:
                column_dict[thing] = {'type': 'text'}
        if isinstance(column_data, str) and column_data != '':
            print("If column_data is a string, it must be '', but this is already the default without passing it in.")
            exit()
        if isinstance(column_data, dict):
            column_dict = column_data
        sql = data_builder(column_dict, sql, 'create')
        sql += f"PRIMARY KEY('{primary_key}' AUTOINCREMENT));"
        executed = self.check_commit(db_alias, sql)
        if not executed:
            print(f"SQL create table, in db with alias {db_alias}, failed: {sql}")
            return False
        return True

    def drop_table(self, db_alias='default', table=''):
        if table == '':
            print("You must enter a table name to drop a table.")
            return False
        if isinstance(db_alias, str) and db_alias == 'default':
            db_alias = self.default_db_info['alias']

        sql = f"DROP TABLE {table}"
        executed = self.check_commit(db_alias, sql)
        if not executed:
            print(f"SQL drop table failed: {sql}")
            return False
        return True

    def truncate_table(self, db_alias='default', table=''):
        if table == '':
            print("You must enter a table name to truncate a table.")
            return False
        if isinstance(db_alias, str) and db_alias == 'default':
            db_alias = self.default_db_info['alias']

        sql = f"DELETE FROM {table}"
        executed = self.check_commit(db_alias, sql)
        if not executed:
            print(f"SQL truncate table failed: {sql}")
            return False
        return True

    def rename_table(self, db_alias='default', table='', new_name=''):
        if table == '':
            print("You must enter a table name to create a table")
            return False
        if isinstance(db_alias, str) and db_alias == 'default':
            db_alias = self.default_db_info['alias']

        if type(table) != str:
            print("You can only rename one table at a time")
            return False
        sql = f"ALTER TABLE {table} RENAME TO {new_name}"
        executed = self.check_commit(db_alias, sql)
        if not executed:
            print(f"SQL rename table failed: {sql}")
            return False
        return True

    def add_column(self, db_alias='default', table='', column_data=''):

        if table == '':
            print("You must enter a table name to create a table")
            return False
        if isinstance(db_alias, str) and db_alias.lower() == 'default':
            db_alias = self.default_db_info['alias']
        if not isinstance(table, str):
            print("You can only alter one table at a time. Table must be a string.")
            return False

        sql = data_builder(column_data, '', 'add')
        sql_split = sql.split('; ')
        for sql in sql_split:
            if sql.lower().strip() != '':
                sql1 = f"ALTER TABLE {table} ADD "
                sql2 = sql1 + sql + ';'
                executed = self.check_commit(db_alias, sql2)
                if not executed:
                    print(f"SQL add column failed: {sql}")
                    return False
        return True

    def drop_column(self, db_alias='default', table='', columns=''):
        if table == '':
            print("You must enter a table name to create a table")
            exit()
        if isinstance(db_alias, str) and db_alias.lower() == 'default':
            db_alias = self.default_db_info['alias']
        if not isinstance(table, str):
            print("You can only alter one table at a time")
            exit()
        if not isinstance(columns, list) and not isinstance(columns, str) and not isinstance(columns, tuple):
            print("column_data must be a string, list or tuple of columns you want to drop from the table")
            exit()
        for column in columns:
            sql = f"ALTER TABLE {table} DROP {column}"
            executed = self.check_commit(db_alias, sql)
            if not executed:
                print(f"SQL drop column failed: {sql}")
                return False
        return True

    def rename_column(self, db_alias='default', table='', columns=''):
        if table == '':
            print("You must enter a table name to create a table")
            exit()
        if isinstance(db_alias, str) and db_alias.lower() == 'default':
            db_alias = self.default_db_info['alias']
        if not isinstance(table, str):
            print("You can only alter one table at a time")
            exit()
        if type(columns) != dict:
            columns = self.dict_checker(columns)
        else:
            if columns == '':
                print("You must choose one or more columns to rename, in the form of a list or tuple of pairs in which "
                      "index 0 is the current column name and index 1 is the new name, or a dictionary in which key is "
                      "current column name and value is new column name.")
                exit()
        for key, value in list(columns.items()):
            sql = f"ALTER TABLE {table} RENAME COLUMN {key} TO {value}"
            executed = self.check_commit(db_alias, sql)
            if not executed:
                print(f"SQL rename column failed: {sql}")
                return False
        return True

    def compare_tables(self, table1='', table2='', columns=''):
        wrong_type_text = "table1 and table2 must be string, list, or tuple. A list or tuple must be a pair where "\
                          "index  0 is the alias of the database and index 1 is the name of the table. Using a "\
                          "string in one table and a list or tuple in the other will use the db alias from the list "\
                          "or tuple for both tables."
        if not table1 or not table2 or table1 == '' or table2 == '':
            print(wrong_type_text)
            exit()
        if (not isinstance(table1, str) and not isinstance(table1, list) and not isinstance(table1, tuple)) or \
                (not isinstance(table2, str) and not isinstance(table2, list) and not isinstance(table2, tuple)):
            print(wrong_type_text)
            exit()

        table_length_text = "If table1 and/or table2 are in the form of a list or tuple, it must be of length 2, " \
                            "where index 0 is the table name and index 1 is the db alias."
        if isinstance(table1, str) and isinstance(table2, str):
            t1_db = t2_db = self.default_db_info['alias']
            tbl1 = table1
            tbl2 = table1
        elif (isinstance(table1, list) or isinstance(table1, tuple)) and (isinstance(table2, str)):
            if len(table1) > 2 or len(table1) < 2:
                print(table_length_text)
                exit()
            t1_db, tbl1 = table1
            tbl2 = table2
            t2_db = t1_db
        elif (isinstance(table1, str)) and (isinstance(table2, list) or isinstance(table2, tuple)):
            if len(table2) > 2 or len(table2) < 2:
                print(table_length_text)
                exit()
            tbl1 = table1
            t2_db, tbl2 = table2
            t1_db = t2_db
        elif (isinstance(table1, list) or isinstance(table1, tuple)) and \
                (isinstance(table2, list) or isinstance(table2, tuple)):
            if len(table1) > 2 or len(table1) < 2 or len(table2) > 2 or len(table2) < 2:
                print(table_length_text)
                exit()
            t1_db, tbl1 = table1
            t2_db, tbl2= table2
        else:
            print(wrong_type_text)
            exit()
        if t1_db.lower() not in self.conn_dict.keys():
            print(f'{t1_db.lower()} was not one of the db aliases you initiated in SQL builder.')
        if t2_db.lower() not in self.conn_dict.keys():
            print(f'{t2_db.lower()} was not one of the db aliases you initiated in SQL builder.')
            exit()
        if tbl1 not in self.conn_dict[t1_db.lower()]['tables']:
            print(f"There is no table called {tbl1} in the database with alias {t1_db.lower()}")
            exit()
        if tbl2 not in self.conn_dict[t2_db.lower()]['tables']:
            print(f"There is no table called {tbl2} in the database with alias {t2_db.lower()}")
            exit()
        if isinstance(columns, str) and columns == '' or columns.lower() == 'all' or columns == '*':
            columns = '*'
        elif isinstance(columns, list) or isinstance(columns, tuple):
            pass
        else:
            print("columns must be a list or tuple of column names or a string with value * or all for all columns")
            exit()
        t1_db_dict = self.conn_dict[t1_db.lower()]
        t2_db_dict = self.conn_dict[t2_db.lower()]
        same = conn.compare(tbl1, t1_db_dict, tbl2, t2_db_dict, columns)
        return same

    def check_commit(self, db_alias, sql):
        if type(db_alias) == str:
            db_alias = db_alias.lower()
            if self.conn_dict[db_alias]['backup']:
                return self.check_commit([db_alias, self.conn_dict[db_alias]['backup']], sql)
            try:
                executed = self.conn_dict[db_alias]['instance'].execute(sql)
                if not executed:
                    return False
            except KeyError:
                print(f"The alias {db_alias} was not found in your initiation of sql builder")
                return False
        elif type(db_alias) == list or isinstance(db_alias, tuple):
            db_alias = list(db_alias)
            errant = [x for x in db_alias if x not in tuple(self.conn_dict.keys())]
            if errant:
                print(f"The following aliases were not found in your initiation of sql builder: {', '.join(errant)}")
                return False

            backups = [self.conn_dict[x]['backup'] for x in db_alias if self.conn_dict[x]['backup']]
            to_execute = set(db_alias + backups)
            for d in to_execute:
                executed = self.conn_dict[d]['instance'].execute(sql)
                if not executed:
                    return False
        else:
            print("db must be a string with a single db alias or a list/tuple with multiple db aliases")
            return False
        return True

    def row_count(self, db_alias='default', table=''):
        if table == '':
            print("You must enter a table name to get the row count")
            return False
        elif type(table) != str:
            print("You can only get the row count of one table at a time")
        if isinstance(db_alias, str) and db_alias.lower() == 'default':
            db_alias = self.default_db_info['alias']
        if not isinstance(db_alias, str) and not isinstance(db_alias, int):
            print("You can only get the row count of one table from one database at a time. The db_alias must be a "
                  "string or integer.")
            exit()
        pk_column = self.find_primary_key(db_alias, table)
        if pk_column:
            sql = f"SELECT MAX({pk_column}) FROM {table}"
            count = self.conn_dict[db_alias]['instance'].fetch(sql)[0][0]
        else:
            count = 0
        return count

    def find_primary_key(self, db, table):
        sql = f"SELECT l.name FROM pragma_table_info('{table}') as l WHERE l.pk = 1;"
        try:
            pk_column = self.conn_dict[db]['instance'].fetch(sql)[0][0]
        except IndexError:
            return False
        return pk_column

    def dict_checker(self, data, db_info=False):
        if not db_info:
            try:
                return dict(data)
            except ValueError:
                print(
                    "The data you provided cannot be turned into a dictionary. INSERT, UPDATE, CREATE, and ALTER data "
                    "must be in a form that can be turned into a dictionary.")
                exit()
            except TypeError:
                print(
                    "The data you provided cannot be turned into a dictionary. INSERT, UPDATE, CREATE, and ALTER data "
                    "must be in a form that can be turned into a dictionary.")
                exit()
        else:
            if not isinstance(data, dict):
                data = self.dict_checker(data)
            found = 0
            for key, value in data.items():
                if isinstance(value, str):
                    data[key] = {'alias': value.lower(),
                                 'backup': None,
                                 'default': False}

                elif isinstance(value, list) or isinstance(value, tuple):
                    data[key] = {}
                    for thing in value:
                        if isinstance(thing, dict) or isinstance(thing, tuple) or isinstance(thing, list):
                            print("You have a value other than a string where there would normally be one to three "
                                  "strings in the following order:\n"
                                  "1. The alias of the database\n"
                                  "2. The alias of the backup for the database or None\n"
                                  "3. True if this is the default database, otherwise False\n")
                            exit()
                        alias, *other = value
                        data[key]['alias'] = alias.lower()

                        if other:
                            backup, *default = other
                            if not backup:
                                data[key]['backup'] = None
                            else:
                                data[key]['backup'] = backup.lower()
                            if default and default[0]:
                                data[key]['default'] = default[0]
                            else:
                                data[key]['default'] = False
                elif isinstance(value, dict):
                    data[key] = {k.lower(): v for k, v in data[key].items()}
                    if 'alias' not in data[key].keys():
                        print("You must choose an alias for each database. Default and backup are optional.")
                        exit()
                    else:
                        data[key]['alias'] = data[key]['alias'].lower()
                    for thing in data[key].keys():
                        if thing not in self.allowed_params:
                            print(f"{thing} is not one of the allowed parameters: {', '.join(self.allowed_params)}")
                            exit()
                    if 'backup' not in data[key].keys():
                        data[key]['backup'] = None
                    else:
                        data[key]['backup'] = data[key]['backup'].lower()
                    if 'default' not in data[key].keys():
                        data[key]['default'] = False
                    else:
                        if not isinstance(data[key]['default'], bool):
                            print("default must be True or False, where True means this database is the default. There "
                                  "can only be one default database per instantiation of SQL Builder.")
                            exit()

            for i, key in enumerate(data):
                if 'backup' not in data[key].keys() or not data[key]['backup'] or data[key]['backup'] == '':
                    data[key]['backup'] = None
                if 'default' not in data[key].keys() or not data[key]['default'] or data[key]['default'] == '':
                    data[key]['default'] = False

            return_dict = {}
            for key in data:
                filename, *ext = key.split('.')
                if not ext:
                    filename = f"{filename}.db"
                else:
                    if ext[-1] != 'db' and ext[-1] != 'sqlite' and ext[-1] != 'db3':
                        if len(ext) > 1:
                            print(
                                "You are either trying to use a period in your filename without specifying .db, .db3, "
                                "or .sqlite or are trying to use an unsupported file type. Leave blank to automatically"
                                " use .db")
                            exit()
                    else:
                        filename = f"{filename}.{'.'.join(ext)}"
                return_dict[data[key]['alias']] = data[key]
                return_dict[data[key]['alias']]['file_path'] = abspath(filename)

                if data[key]['default']:
                    found += 1

            if found > 1:
                print("You can only have one default database.")
                exit()
            return return_dict
