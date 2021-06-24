# Task 1
# For first 1000 companies
# Load company_dim, security_dim and then clean_financial_fact

def CreateSnowflakeDBandSchema (
    sfPswd = '',
    sqPswd = '',
    sfAccount = 'myAccount.my-region',
    sfUser = 'my_user',
    sfDatabase = 'DEMO_DB',
    sfSchema = 'DEMO_SCHEMA',
    sqUser = 'root',
    sqHost = 'localhost'
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
          port="1983"
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

    # <--------------------Command Execution-------------------->
    import pandas as pd
    from snowflake.connector.pandas_tools import write_pandas
    # Getting table company_dim
    sqq.execute("USE qmscore")
    sqq.execute("SELECT * FROM company_dim LIMIT 1000")
    table_rows = sqq.fetchall()
    df = pd.DataFrame(table_rows)
    print("Obtained rows from MySQL and converted into Pandas dataframe")
    print(df)
    #print(output)
    #import csv
    #with open("out.csv", "w", newline='') as csv_file: 
    #        csv_writer = csv.writer(csv_file)
    #        csv_writer.writerow([i[0] for i in sqq.description]) # write headers
    #        csv_writer.writerows(sqq)

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

    # <--------------------Command Execution-------------------->
    from snowflake.sqlalchemy import URL
    from sqlalchemy import create_engine
    sfq.execute("USE ROLE DEMO_ACCESS")
    sfq.execute("USE WAREHOUSE COMPUTE_WH")
    sfq.execute("USE DATABASE DEMO_DB")
    sfq.execute("USE SCHEMA DEMO_DB.TESTANALYTICS")
    query = """CREATE OR REPLACE TABLE company_dim (
  company_seq_id smallint NOT NULL,
  company_name varchar(250) DEFAULT NULL,
  company_cik int DEFAULT NULL,
  company_primary_symbol varchar(20) DEFAULT NULL,
  company_short_name varchar(100) DEFAULT NULL,
  company_long_name varchar(250) DEFAULT NULL,
  company_category_code varchar(5) DEFAULT NULL,
  company_category varchar(50) DEFAULT NULL,
  country_seq_id smallint DEFAULT NULL,
  company_ind_class_system varchar(200) DEFAULT NULL,
  company_sic varchar(300) DEFAULT NULL,
  company_sector_type varchar(20) DEFAULT NULL,
  company_sector varchar(50) DEFAULT NULL,
  company_industry varchar(100) DEFAULT NULL,
  company_sub_industry varchar(100) DEFAULT NULL,
  last_fiscal_year_end date DEFAULT NULL,
  fiscal_adjustment tinyint DEFAULT NULL,
  quarter_adjustment tinyint DEFAULT NULL,
  reporting_currency varchar DEFAULT NULL,
  PRIMARY KEY (company_seq_id) )
    """
    #sfq.execute(query)
    #success, nchunks, nrows, _ = write_pandas(conn=sfConnection,df=df,table_name='company_dim')
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
    print("Engine created")
    df.to_sql('company_dim', con=engine, index=False)
    print("Dataframe loaded into Snowflake")
    engineConnection.close()
    engine.dispose()
    print('Steps complete')
    sqq.close()
    sqConnection.close()
    sfq.close()
    sfConnection.close()

CreateSnowflakeDBandSchema (
    sfPswd = '',
    sqPswd = '',
    sfAccount = 'bqa34388',
    sfUser = 'USER_1',
    sfDatabase = 'DEMO_DB',
    sfSchema = 'TESTANALYTICS',
    sqUser = 'qm_readonly',
    sqHost = 'qmanalyticsdb.cr4wvcewigkc.us-west-2.rds.amazonaws.com'
)
