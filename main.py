from sqlalchemy import create_engine
import pandas
import snowflake.connector
from dbnd import *
from dbnd import log_dataframe, log_metric
#from dbnd import log_snowflake_resouce_usage, log_snowflake_table
import datetime
import credentials
import sys, psycopg2

def establish_postgres_connection():
    '''uses sqlalchemy to establish connection to postgresql db'''
    #pg_db = create_engine('postgresql+psycopg2:// ec2-52-70-67-123.compute-1.amazonaws.com'

    engine = create_engine("postgresql+psycopg2://ugrebibjthdozz:1edde6187d75c2c50b4ef0476f58b33cbfeb70f41da88014897a86439432e357@ec2-52-70-67-123.compute-1.amazonaws.com/ddoa83tf33ujai")
    return engine

def establish_snowflake_connection():
    '''uses Snowflake Python Connector to establish connection to snowflake, returns connection and connection string'''
    snowflake_connection = snowflake.connector.connect(
        user='wgumaru',
        password='Jesus4Wcg',
        account='yoa32090',
        database= 'DEV',
        schema= 'STG_PSAS'
    )
    connection_string = f'snowflake://{"wgumaru"}:{"Jesus4Wcg"}@{"yoa32090.snowflakecomputing.com"}/?account={"yoa32090"}'
    return snowflake_connection, connection_string

path = "\\"
path_tuple = ("C:", "tmp","transactions.csv")
staging_file = path.join(path_tuple)
staging_file = "C:\\Users\\Kris\\PycharmProjects\\pythonProjectPG\\transactions.csv"
conn = psycopg2.connect(host="ec2-52-70-67-123.compute-1.amazonaws.com",
                        user="ugrebibjthdozz",
                        password="1edde6187d75c2c50b4ef0476f58b33cbfeb70f41da88014897a86439432e357",
                        dbname= "ddoa83tf33ujai",
                        port=5432
                        )
query = "COPY (SELECT * FROM ASSESSMENTS_DOCTOR) TO '{staging_file}' WITH DELIMITER ',' CSV HEADER;"

query = "COPY (SELECT * FROM ASSESSMENTS_DOCTOR) TO STDOUT WITH CSV DELIMITER ','"

cur = conn.cursor()
with open("C:\\tmp\\transactions.csv","w") as file:
    cur.copy_expert(query,file)
#print(query)
#establish_snowflake_connection()
#print("hello")


conn = getConnectiom()
createFile(conn,tableName)

export()
