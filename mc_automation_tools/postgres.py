"""
This module adapt and provide useful access to postgresSQL DB
"""
import logging
import psycopg2

_log = logging.getLogger('mc_automation_tools.postgres')


class PGClass:
    """
    This class create and provide connection to postgres db host
    """

    def __init__(self, host, database, user, password, scheme, port=5432):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.scheme = scheme

        try:
            self.conn = psycopg2.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password,
                port=self.port)
        except Exception as e:
            raise ConnectionError(f'Error on connection to DB with error: {str(e)}')


    def get_SqlHelper(self):# Method to send to inner Class SqlHelper Connection parameters.
        return  self.SqlHelper(self.conn, self.scheme,self.database)

    # Read me SqlHelper class can execute Sql statement in 2 ways
    # 1.0v by Shay Perpinial

    # Create a PGClass instance
    # get_SqlHelper method from PGClass return instance of SqlHelper cLass with the parameters.
    # From the object can produce with build_query , sending the parameters by
    #       - "results_to_get" = Select  can get None as a default and get all the parameters as *
    #       - "table_name" = From The table that get the data from as format  "SchemasName".TableName
    #       - "search_data" = The data that we compare to column name .
    #       - "data" = Columns  name from the table that we compare to the Data
    #       - "operator" =  there is 4  operator available  = ('compare', 'start', 'end', 'have')  send a string of the
    #                       desire operation start/ end  "Like" operation for start with and ending string .

    #    execute_one() return a list of item as a tuple, if only 1 item return will be tuple inside list
    #    execute_all() return a list of item as a tuple, if only 1 item return will be tuple inside list

    #    unit_dict()   method instead of  execute_one/all  return a dictionary key/value of the return statement

    #    execute_sql_statement get a String query return the results  without get a parameters. o
    #    ptional send after the query, true for get as a dictionary

    class SqlHelper:
        def __init__(self, conn,scheme,database):
            self.conn = conn
            self.scheme =  scheme
            self.database = database
            query = str()

        def build_query(self, results_to_get, table_name, search_data, data, operator, as_dict=False):
            self.table = table_name
            condition = []
            value = results_to_get if results_to_get is not None else '*'
            self.query = f"""SELECT {value} FROM {table_name} WHERE """
            condition.append(search_data)
            condition.append(data)
            self.query += self.__operation(operator, condition)
            print(self.query)

        def __compare(self, var_condition):
            condition = f"{var_condition[0]} = '{var_condition[1]}'"
            return condition

        def __start_with(self, var_condition):
            statement = f""""{var_condition[0]}" LIKE '{var_condition[1]}%'"""
            return statement

        def __end_with(self, var_condition):
            statement = f"{var_condition[0]} LIKE '%{var_condition[1]}'"
            return statement

        def __have_in_query(self, var_condition):
            statement = f"{var_condition[0]} LIKE '%{var_condition[1]}%' "
            return statement

        def execute_one(self): # return a list of item as a tuple, if only 1 item return will be tuple inside list
            try:
                cur = self.conn.cursor()
                cur.execute(self.query)
            except Exception as e:
                logging.exception(e)
                return cur.fetchone()
            finally:
                cur.close()


        def execute_all(self): # return a list of item as a tuple, if only 1 item return will be tuple inside list
            try:
                cur = self.conn.cursor()
                cur.execute(self.query)
            except Exception as e:
                logging.exception(e)
                return cur.fetchall()
            finally:
                cur.close()

        # Function to decide each operator use on the statement
        def __operation(self, op, condition):
            operation_dict = {'compare': self.__compare(condition),
                              'start': self.__start_with(condition),
                              'end': self.__end_with(condition),
                              'have': self.__have_in_query(condition)
                              }
            return operation_dict[op]

        def order_by_desc(self, variable):  # default
            self.query += f" ORDER BY {variable} DESC"

        def order_by_asc(self, variable):
            self.query += f" ORDER BY {variable} ASC"

        def unit_dict(self):
            res = self._list_of_column()
            query_list = self.execute_one()
            return dict(zip(res, query_list))

        def _list_of_column(self):
            cur = self.conn.cursor()
            sql = f"""SELECT * FROM {self.table} """
            cur.execute(sql)
            column_names = [desc[0] for desc in cur.description]
            print(column_names)
            self.conn.commit()
            #       self.conn.close()
            return column_names

        def execute_sql_statement(self, query,as_dict=False):
            try:
                cur = self.conn.cursor()
                cur.execute(query)
            except Exception as e:
                logging.exception(e)
            if not as_dict:
                return cur.fetchall()
            else:
                res = self._list_of_column()
                query_list=cur.fetchone()
                return dict(zip(res, query_list))

    def command_execute(self, commands):
        try:
            cur = self.conn.cursor()

            for command in commands:
                cur.execute(command)
            cur.close()


            self.conn.commit()

        except (Exception, psycopg2.DatabaseError) as e:
            _log.error(str(e))
            raise e
        #optional
        # try:
        #     cur = self.conn.cursor()
        #     if isinstance(commands, list):
        #         print("your object is a list !")
        #         for command in commands:
        #             cur.execute(command)
        #         return cur.fetchall()
        #     else:
        #         print("your object is not a list")
        #         cur.execute(commands)
        #         return cur.fetchone()
        #
        # except (Exception, psycopg2.DatabaseError) as e:
        #     _log.error(str(e))
        #     raise e
        #
        # finally:
        #     if self.conn:
        #         self.conn.close()


    def get_column_by_name(self, table_name, column_name):
        """
        This method return list of column data by providing column name
        """
        command = f"""SELECT {column_name} FROM "{self.scheme}"."{table_name}";"""
        try:
            cur = self.conn.cursor()
            cur.execute(command)
            res = cur.fetchall()
            cur.close()
        except Exception as e:
            _log.error(str(e))
            raise e
        return [var[0] for var in res]

    def update_value_by_pk(self, pk, pk_value, table_name, column, value):
        """This method will update column by provided primary key and table name """
        command = f"""UPDATE "{self.scheme}"."{table_name}" SET "{column}"='{value}' WHERE {pk} = '{pk_value}';"""
        try:
            cur = self.conn.cursor()
            cur.execute(command)

            self.conn.commit()
            cur.close()

        except Exception as e:
            _log.error(str(e))
            raise e

    def polygon_to_geojson(self, column, table_name, pk, pk_value):
        """
        This method query for geometry object and return as geojson format readable
        """
        command = f"""select st_AsGeoJSON({column}) from "{self.scheme}"."{table_name}" where {pk}='{pk_value}';"""
        try:
            cur = self.conn.cursor()
            cur.execute(command)
            res = cur.fetchall()
            cur.close()
        except Exception as e:
            _log.error(str(e))
            raise e

        return res

    def delete_row_by_id(self, table_name, pk, pk_value):
        """
        Delete entire row by providing key and value [primary key]
        """
        command = f"""delete from "{self.scheme}"."{table_name}" where "{pk}"='{pk_value}';"""
        try:
            cur = self.conn.cursor()
            cur.execute(command)
            self.conn.commit()
            resp = cur.statusmessage
            cur.close()
            return resp

        except Exception as e:
            _log.error(str(e))
            raise e

    def drop_table(self, table_name):
        """
        This method will drop table by providing name of table to drop
        """
        command = f"""DROP TABLE "{self.scheme}"."{table_name}" CASCADE;"""
        cur = self.conn.cursor()
        cur.execute(command)
        self.conn.commit()
        cur.close()

    def truncate_table(self, table_name):
        """
        This method will empty and remove all rows on table by providing name of table to drop
        """
        command = f"""TRUNCATE TABLE "{self.scheme}"."{table_name}";"""
        cur = self.conn.cursor()
        cur.execute(command)
        self.conn.commit()
        cur.close()

    def get_by_json_key(self, table_name, pk, canonic_keys, value):
        """
        This method send query with canonic search over multiple number of pk arguments and return rows include value:
            Sample: the canonic =====> root_key->some_sub_key->optional_sub_key......-><value expected>
        :param table_name: table name
        :param pk: primary key
        :param canonic_keys: list of arguments represent dict canonic order of keys
        :param value: column name to get
        """

        search_str = '->'.join(["'"+c+"'" for c in canonic_keys])
        command = f"""select * from "{self.scheme}"."{table_name}" where "{pk}"->{search_str}->'{value}' is not NULL;"""
        try:
            cur = self.conn.cursor()
            cur.execute(command)
            res = cur.fetchall()
            cur.close()
        except Exception as e:
            _log.error(str(e))
            raise e
        return res

    def delete_by_json_key(self, table_name, pk, canonic_keys, value):
        """
        This method send query with canonic search over multiple number of pk arguments and return rows include value:
            Sample: the canonic =====> root_key->some_sub_key->optional_sub_key......-><value expected>
        :param table_name: table name
        :param pk: primary key
        :param canonic_keys: list of arguments represent dict canonic order of keys
        :param value: column name to get
        """

        search_str = '->'.join(["'"+c+"'" for c in canonic_keys])
        command = f"""delete from "{self.scheme}"."{table_name}" where "{pk}"->{search_str}->'{value}' is not NULL;"""
        try:
            cur = self.conn.cursor()
            cur.execute(command)
            self.conn.commit()
            resp = cur.statusmessage
            cur.close()
            return resp
        except Exception as e:
            _log.error(str(e))
            raise e


    def get_by_n_argument(self, table_name, pk, pk_values, column):
        """
        This method send query with multiple number of pk arguments
        :param table_name: table name
        :param pk: primary key
        :param pk_values: list of arguments
        :param column: column name to get
        """
        for idx, i in enumerate(pk_values):
            pk_values[idx] = f"'{i}'"
        args_str = ','.join(pk_values)
        command = f"""select "{column}" from "{self.scheme}"."{table_name}" where "{pk}" in ({args_str})"""
        try:
            cur = self.conn.cursor()
            cur.execute(command)
            res = cur.fetchall()
            cur.close()
        except Exception as e:
            _log.error(str(e))
            raise e
        res = [r[0] for r in res]
        return res

    def update_multi_with_multi(self, table_name, pk, column, values, type_pk, type_col):
        """
         Insert multiple rows with one query
        """
        update_query = f"""with vals (i,j) as (values"""
        for v in values:
            update_query = update_query + f""" (cast('{v[0]}' as {type_pk}),cast('{v[1]}' as {type_col})),"""
            # update_query = update_query + f""" ('{v[0]}' ,'{v[1]}'),"""
        update_query = update_query[:-1]+")"
        update_query = update_query + f""" update "{self.scheme}"."{table_name}" as my_table set "{column}" = vals.j from vals where my_table.{pk} = vals.i;"""
        # update = f"""update {table_name} set {column} = vals.j from vals where {table_name}.{pk} = vals.i;"""

        # insert_query = f"""insert into {table_name} ({pk}, {column}) values {records_list_template}"""
        cur = self.conn.cursor()
        cur.execute(update_query)
        # cur.execute(update)

        self.conn.commit()
        cur.close()

    def get_rows_by_order(self, table_name, order_key=None, order_desc=False, return_as_dict=False):
        """
        This method will query for entire table rows order by specific parameter
        """

        asc_desc = 'asc' if not order_desc else 'desc'
        command = f"""select * from "{self.scheme}"."{table_name}" order by "{order_key}" {asc_desc}"""

        try:
            cur = self.conn.cursor()
            cur.execute(command)
            if return_as_dict:
                columns = list(cur.description)
                res = cur.fetchall()

                results = []
                for row in res:
                    row_dict = {}
                    for i, col in enumerate(columns):
                        row_dict[col.name] = row[i]
                    results.append(row_dict)

                return results

            else:
                res = cur.fetchall()
                cur.close()
                return res
        except Exception as e:
            _log.error(str(e))
            raise e

    def get_rows_by_keys(self, table_name, keys_values, order_key=None, order_desc=False, return_as_dict=False):
        """
        This method returns rows that suitable on several keys-values
        :param table_name: table name
        :param keys_values: list of dict with columns keys values
        :param order_key: str of key for ordering query - not mendatory
        :param order_desc: order method - not mendatory as default ASC
        :param return_as_dict: bool -> if return the result as dict or list
        """

        key_value_stat = []
        for key, val in keys_values.items():
            key_value_stat.append(f""""{key}" = '{val}'""")

        key_value_stat = """ and """.join(key_value_stat)

        if not order_key:
            command = f"""select * from "{self.scheme}"."{table_name}" where ({key_value_stat})"""
        else:
            asc_desc = 'asc' if not order_desc else 'desc'
            command = f"""select * from "{self.scheme}"."{table_name}" where ({key_value_stat}) order by "{order_key}" {asc_desc}"""
        try:
            cur = self.conn.cursor()
            cur.execute(command)
            if return_as_dict:
                columns = list(cur.description)
                res = cur.fetchall()

                results = []
                for row in res:
                    row_dict = {}
                    for i, col in enumerate(columns):
                        row_dict[col.name] = row[i]
                    results.append(row_dict)

                return results

            else:
                res = cur.fetchall()
                cur.close()
                return res
        except Exception as e:
            _log.error(str(e))
            raise e
        # res = [r[0] for r in res]
        # return res



    # def create_table(self, table_name, primary_key, columns):
    #     """
    #     This method add new table according to provided table_name and
    #     :param table_name: name of new table to create - <str>
    #     :param primary_key: name of PRIMARY_KEY - list of tuples - [(primary_key_str, data_type)]
    #     :param columns: name of other columns + foreign - list of tuples tuple - [(column name_str, data_type str, NULL - True\False, is foreign)]
    #     """
    #     prefix = f"CREATE TABLE {table_name}"
    #
    #     primary_keys_list = []
    #     for key in primary_key:
    #         primary_keys_content = ""
    #         for var in key:
    #             primary_keys_content + " " + var
    #         primary_keys_content + 'PRIMARY KEY'
    #
    #     for key in columns:
    #
    #     pass
