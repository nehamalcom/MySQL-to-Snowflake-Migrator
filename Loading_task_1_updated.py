# Task 1
# For first 1000 companies
# Load company_dim, security_dim and then clean_financial_fact
import snowflake.connector as sf
import getpass
import pandas as pd
import json
from datetime import datetime 
from snowflake.connector.pandas_tools import write_pandas
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine

def mysqlConnector (
    sqPswd,
    sqUser,
    sqHost,
    sqPort
):
    # <--------------------MySQL connection setup-------------------->
    import mysql.connector as sq
    if sqPswd == '' :
      import getpass
      sqPswd = getpass.getpass('MySQL Password:')
    # Test the connection to MySQL
    try:
      sqConnection = sq.connect(
          user=sqUser,
          password=sqPswd,
          host=sqHost,
          port=sqPort
      )
      sqq = sqConnection.cursor(buffered=True)
      print("Connected to MySQL")

      return (sqConnection, sqq)
      
    except:
      print('Connection to MySQL failed. Check credentials')
    # Open connection to MySQL
   
    


def snowflakeConnector (
    sfPswd,
    sfUser,
    sfDatabase,
    sfSchema,
    sfAccount
):
    # <--------------------Snowflake connection setup-------------------->
   
    # Request user password if not provided already
    if sfPswd == '' :
      
      sfPswd = getpass.getpass('Snowflake Password:')
    
    try:
      # Open connection to Snowflake
      sfConnection = sf.connect(
          user=sfUser,
          password=sfPswd,
          account=sfAccount
      )
      sfq = sfConnection.cursor()
      sfq = sfConnection.cursor()
      print("Connected to Snowflake")

      return (sfConnection, sfq)
    except:
      print('Connection to Snowflake failed. Check credentials')

def dataLoadFromMySQLtoSnowflake (
    sqUser = 'root',
    sqPswd = '',
    sqHost = 'localhost',
    sqPort = '3306',
    sqDatabase = 'DEMO_DB',
    sfUser = 'my_user',
    sfPswd = '',
    sfAccount = 'myAccount.my-region',
    sfRole = 'PUBLIC',
    sfDatabase = 'DEMO_DB',
    sfSchema = 'DEMO_SCHEMA',
    sfWarehouse = 'COMPUTE_WH'
):
    # Connect to MySQL
    sqConnection, sqq = mysqlConnector (
        sqPswd = sqPswd,
        sqUser = sqUser,
        sqHost = sqHost,
        sqPort = sqPort
    )
    # Connect to Snowflake
    sfConnection, sfq = snowflakeConnector (
        sfUser = sfUser,
        sfPswd = sfPswd,
        sfAccount = sfAccount,
        sfDatabase = sfDatabase,
        sfSchema = sfSchema
    )
    
    # Setting up Snowflake to load into
    sfq.execute(f"USE ROLE {sfRole}")
    sfq.execute(f"USE WAREHOUSE {sfWarehouse}")
    sfq.execute(f"USE DATABASE {sfDatabase}")
    sfq.execute(f"USE SCHEMA {sfDatabase}.{sfSchema}")
    engine = create_engine(URL(
        account = sfAccount,
        user = sfUser,
        password = sfPswd,
        database = sfDatabase,
        schema = sfSchema,
        warehouse = sfWarehouse,
        role=sfRole
    ))
    engineConnection = engine.connect()
    # Getting data from MySQL and loading into Snowflake
    sqq.execute(f"USE {sqDatabase}")
    table_name = ""
    print("Enter table to be loaded (enter 'stop' to exit)")
    while True:
        table_name = input("Table Name: ")
        if (table_name=='stop'):
            break
        sqq.execute(f"SELECT * FROM {table_name} LIMIT 1000")
        table_rows = sqq.fetchall()
        table_headers = sqq.column_names
        df = pd.DataFrame(table_rows,columns=table_headers)
        #df = pd.DataFrame(table_rows)
        try:
            try:
                #loading data using to_sql method when new table is to be created
                begin_time = datetime.now()
                df.to_sql(table_name, con=engine, index=False)
                end_time = datetime.now()
                print(f"{table_name} loaded into Snowflake in {end_time-begin_time} using to_sql method")
            except:
                # loading data into an existing table using write_pandas function
                begin_time = datetime.now()
                table_name=table_name.upper()
                write_pandas(sfConnection,df,table_name,database="DEMO_DB",schema="TESTANALYTICS")
                end_time = datetime.now()
                print(f"{table_name} loaded into Snowflake in {end_time-begin_time} using  write_pandas method")

        except:
            print("Table cannot be created")

    
    engineConnection.close()
    engine.dispose()
    print('Steps complete')
    sqq.close()
    sqConnection.close()
    sfq.close()
    sfConnection.close()


def configure():
    with open('config.json') as config_file:
        data = json.load(config_file)
    global sqUser
    global sqPswd 
    global sqHost
    global sqPort
    global sqDatabase
    global sfUser
    global sfPswd
    global sfAccount
    global sfRole
    global sfDatabase
    global sfSchema
    global sfWarehouse
    sqUser = data['MYSQL']['USERNAME']
    sqPswd = data['MYSQL']['PASSWORD']
    sqHost = data['MYSQL']['HOST']
    sqPort = data['MYSQL']['PORT']
    sqDatabase = data['MYSQL']['DATABASE']
    sfUser = data['SNOWFLAKE']['USERNAME']
    sfPswd = data['SNOWFLAKE']['PASSWORD']
    sfAccount = data['SNOWFLAKE']['ACCOUNT']
    sfRole = data['SNOWFLAKE']['ROLE']
    sfDatabase = data['SNOWFLAKE']['DATABASE']
    sfSchema = data['SNOWFLAKE']['SCHEMA']
    sfWarehouse = data['SNOWFLAKE']['WAREHOUSE']



if __name__=="__main__":
    # getting required configuration settings from config.json
    configure()
    # function to load data from MySQL server to Snowflake
    dataLoadFromMySQLtoSnowflake (
        sqUser = sqUser,
        sqPswd = sqPswd,
        sqHost = sqHost,
        sqPort = sqPort,
        sqDatabase = sqDatabase,
        sfUser = sfUser,
        sfPswd = sfPswd,
        sfAccount = sfAccount,
        sfRole = sfRole,
        sfDatabase = sfDatabase,
        sfSchema = sfSchema,
        sfWarehouse = sfWarehouse
    )

