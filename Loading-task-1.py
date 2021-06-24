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
          port="1983"
      )
    sqq = sqConnection.cursor(buffered=True)
    print("Connected to MySQL")

    # <--------------------Command Execution-------------------->
    # Getting table company_dim
    sqq.execute("USE qmscore")
    sqq.execute("SELECT * FROM company_dim LIMIT 1000")
    #output = sqq.fetchall()
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
    sfq.execute("USE ROLE DEMO_ACCESS")
    sfq.execute("USE WAREHOUSE COMPUTE_WH")
    sfq.execute("USE DATABASE DEMO_DB")
    sfq.execute("USE SCHEMA DEMO_DB.TESTANALYTICS")
    sfq.execute("CREATE OR REPLACE TABLE abc_table(name varchar(20), age integer)")

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
