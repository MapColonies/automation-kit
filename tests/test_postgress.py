"""Unittest for postgress client adapter"""
import os
from mc_automation_tools import postgress
user = os.environ['PG_USER']
password = os.environ['PG_PASS']
host = os.environ['PG_HOST']
db_name = os.environ['PG_DB_NAME']
table_name = 'test_full'

def test_connection():
    """This test check connection to db"""
    postgress.PGClass(host, db_name, user, password)
    assert True


test_connection()