# Task 1
# For first 1000 companies
# Load company_dim, security_dim and then clean_financial_fact

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
      sqq.close()
      sqConnection.close()
    except:
      print('Connection to MySQL failed. Check credentials')
    # Open connection to MySQL
    sqConnection = sq.connect(
          user=sqUser,
          password=sqPswd,
          host=sqHost,
          port="1983",
          database="qmscore"
      )
    sqq = sqConnection.cursor(buffered=True)
    print("Connected to MySQL")

    return (sqConnection, sqq)


def snowflakeConnector (
    sfPswd = '',
    sfUser = 'my_user',
    sfDatabase = 'DEMO_DB',
    sfSchema = 'DEMO_SCHEMA',
    sfAccount = ''
):
    # <--------------------Snowflake connection setup-------------------->
    import snowflake.connector as sf
    # Request user password if not provided already
    if sfPswd == '' :
      import getpass
      sfPswd = getpass.getpass('Snowflake Password:')
    # Test the connection to Snowflake
    try:
      sfConnection = sf.connect(
          user=sfUser,
          password=sfPswd,
          account=sfAccount
      )
      sfq = sfConnection.cursor()
      sfq.close()
      sfConnection.close()
    except:
      print('Connection to Snowflake failed. Check credentials')
    # Open connection to Snowflake
    sfConnection = sf.connect(
      user=sfUser,
      password=sfPswd,
      account=sfAccount
    )
    sfq = sfConnection.cursor()
    print("Connected to Snowflake")

    return (sfConnection, sfq)
    

def dataLoadFromMySQLtoSnowflake (
    sfPswd = '',
    sqPswd = '',
    sfAccount = 'myAccount.my-region',
    sfUser = 'my_user',
    sfDatabase = 'DEMO_DB',
    sfSchema = 'DEMO_SCHEMA',
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
    import pandas as pd
    from datetime import datetime 
    from snowflake.connector.pandas_tools import write_pandas
    from snowflake.sqlalchemy import URL
    from sqlalchemy import create_engine
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
        warehouse = 'COMPUTE_WH',
        role='DEMO_ACCESS',
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
        sqq.execute(f"SELECT * FROM {table_name} LIMIT 1000")
        table_rows = sqq.fetchall()
        table_headers = sqq.column_names
        df = pd.DataFrame(table_rows,columns=table_headers)
        #df = pd.DataFrame(table_rows)
        begin_time = datetime.now()
        df.to_sql(table_name, con=engine, index=False)
        end_time = datetime.now()
        print(f"{table_name} loaded into Snowflake in {end_time-begin_time}")

    
    engineConnection.close()
    engine.dispose()
    print('Steps complete')
    sqq.close()
    sqConnection.close()
    sfq.close()
    sfConnection.close()

dataLoadFromMySQLtoSnowflake (
    sfPswd = 'Dpa@1234',
    sqPswd = 'qmreadonly2020',
    sfAccount = 'bqa34388',
    sfUser = 'USER_1',
    sfDatabase = 'DEMO_DB',
    sfSchema = 'TESTANALYTICS',
    sqUser = 'qm_readonly',
    sqHost = 'qmanalyticsdb.cr4wvcewigkc.us-west-2.rds.amazonaws.com'
)

