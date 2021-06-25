# Task 1
# For first 1000 companies
# Load company_dim, security_dim and then clean_financial_fact
import snowflake.connector as sf
import getpass
import pandas as pd
from datetime import datetime 
from snowflake.connector.pandas_tools import write_pandas
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine


def mysqlConnector (
    sqPswd = '',
    sqUser = 'root',
    sqHost = 'localhost',
    sqPort = '1983'
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
    sfPswd = '',
    sfUser = 'my_user',
    sfDatabase = 'DEMO_DB',
    sfSchema = 'DEMO_SCHEMA',
    sfAccount = ''
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
    sfPswd = '',
    sqPswd = '',
    sfAccount = 'myAccount.my-region',
    sfUser = 'my_user',
    sfDatabase = 'DEMO_DB',
    sfSchema = 'DEMO_SCHEMA',
    sfwarehouse='COMPUTE_WH',
    sfrole = 'DEMO_ACCESS',
    sqUser = 'root',
    sqHost = 'localhost',
    sqPort = '1983'
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
        sfPswd = sfPswd,
        sfUser = sfUser,
        sfDatabase = sfDatabase,
        sfSchema = sfSchema,
        sfAccount = sfAccount
    )
    
    # Setting up Snowflake to load into
    sfq.execute("USE ROLE DEMO_ACCESS")
    sfq.execute("USE WAREHOUSE COMPUTE_WH")
    sfq.execute("USE DATABASE DEMO_DB")
    sfq.execute("USE SCHEMA DEMO_DB.TESTANALYTICS")
    engine = create_engine(URL(
        account = sfAccount,
        user = sfUser,
        password = sfPswd,
        database = sfDatabase,
        schema = sfSchema,
        warehouse = sfwarehouse,
        role=sfrole,
    ))
    engineConnection = engine.connect()
    # Getting data from MySQL and loading into Snowflake
    sqq.execute("USE qm_score_prod")
    table_name = ""
    print("Enter table to be loaded (enter 'stop' to exit)")
    while True:
        table_name = input("Table Name: ")
        if (table_name=='stop'):
            break
        try:
            sqq.execute(f"SELECT * FROM {table_name} LIMIT 1000")
        except mysql.connector.Error as err:
            print(f"Error in fetching data from {table_name}")
            print(err)
            
        table_rows = sqq.fetchall()
        table_headers = sqq.column_names
        
        for i in range(len(table_headers)):
            table_headers[i]=table_headers[i].upper()
        df = pd.DataFrame(table_rows,columns=table_headers)
        
        
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
            try:
                write_pandas(sfConnection,df,table_name,database=sfdatabase,schema=sfschema)
            except snowflake.connector.error as err:
                print("error in loading data to snowflake")
                print(err)
            end_time = datetime.now()
            print(f"{table_name} loaded into Snowflake in {end_time-begin_time} using  write_pandas method")
            

    
    engineConnection.close()
    engine.dispose()
    print('Steps complete')
    sqq.close()
    sqConnection.close()
    sfq.close()
    sfConnection.close()
    

if __name__=="__main__":
     #function to load data from MySQL server to Snowflake
    dataLoadFromMySQLtoSnowflake (
        sfPswd = 'Dpa@1234',
        sqPswd = 'qmreadonly2020',
        sfAccount = 'bqa34388',
        sfUser = 'USER_1',
        sfDatabase = 'DEMO_DB',
        sfSchema = 'TESTANALYTICS',
        sfwarehouse='COMPUTE_WH',
        sfrole = 'DEMO_ACCESS',
        sqUser = 'qm_readonly',
        sqHost = 'qmanalyticsdb.cr4wvcewigkc.us-west-2.rds.amazonaws.com'
    )

