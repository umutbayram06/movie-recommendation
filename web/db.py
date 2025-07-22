import psycopg
from config import conn_string

def get_connection():
    return psycopg.connect(conn_string, row_factory=psycopg.rows.dict_row)
