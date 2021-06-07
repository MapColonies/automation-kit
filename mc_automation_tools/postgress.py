"""
This module adapt and provide useful access to postgressSQL DB
"""
import psycopg2


class PGClass:
    """
    This class create and provide connection to postgress db host
    """
    def __init__(self, host, database, user, password):
        self.host = host
        self.database = database
        self.user = user
        self.password = password

        try:
            self.conn = psycopg2.connect(
                        host=self.host,
                        database=self.database,
                        user=self.user,
                        password=self.password)
        except Exception as e:
            raise ConnectionError(f'Error on connection to DB with error: {str(e)}')


    def create_table(self, table_name, primary_key, columns, foreign_key=None):
        """
        This method add new table according to provided table_name and
        :param table_name: name of new table to create - <str>
        :param primary_key: name of PRIMARY_KEY - tuple - (primary_key_str, data_type)
        :param primary_key: name of PRIMARY_KEY - list of tuples tuple - [(column name_str, data_type str, NULL - True\False)]
        """
        pass