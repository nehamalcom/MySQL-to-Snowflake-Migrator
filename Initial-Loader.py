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

    # <--------------------MySQL connection setup-------------------->
    import mysql.connector as sq
    if sqPswd == '' :
      import getpass
      sqPswd = getpass.getpass('MySQL Password:')
    # Test the connection to MySQL
    try:
      sqConnection = sq.connect(
          user='root',
          password=sqPswd,
          host='localhost'
      )
      sqq = sqConnection.cursor()
      sqq.close()
      sqConnection.close()
    except:
      print('Connection to MySQL failed. Check credentials')
    # Open connection to MySQL
    sqConnection = sq.connect(
          user=sqUser,
          password=sqPswd,
          host=sqHost
      )
    sqq = sqConnection.cursor()

    # <--------------------Command Execution for Loading-------------------->
    import csv
    #DATABASE CREATION IN MYSQL
    sqq.execute("CREATE DATABASE test_db ")
    print ("MySQL: Database test_db created.")
    sqq.execute("USE test_db ")
    sqq.execute("CREATE TABLE test_table (name varchar(20), age integer)")
    print("MySQL: Table created.")
    sqq.execute("INSERT INTO test_table (name, age) VALUES ('John', 20)")
    sqq.execute("INSERT INTO test_table (name, age) VALUES ('Clare', 47)")
    sqq.execute("INSERT INTO test_table (name, age) VALUES ('Raju', 50)")
    #sqq.execute("DROP DATABASE test_db ")
    #print ("MySQL: Database test_db dropped.")
    #sqq.execute("SHOW DATABASES ")
    #databases = sqq.fetchall()
    #for database in databases:
    #    print(database)
    #sqq.execute("SHOW TABLES;")
    #tables = sqq.fetchall()
    #for table in tables:
        #sqq.execute(f"select * from {table}")
        #with open("out.csv", "w", newline='') as csv_file: 
        #    csv_writer = csv.writer(csv_file)
        #    csv_writer.writerow([i[0] for i in sqq.description]) # write headers
        #    csv_writer.writerows(sqq)
    sqq.execute("SELECT * FROM test_table")
    with open("out.csv", "w", newline='') as csv_file: 
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow([i[0] for i in sqq.description]) # write headers
            csv_writer.writerows(sqq)
    
    
    # LOADING CSV INTO SNOWFLAKE
    sfq.execute("CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH")
    sfq.execute("CREATE DATABASE IF NOT EXISTS DEMO_DB")
    #sfq.execute("USE ROLE SECURITYADMIN")
    #sfq.execute("GRANT MANAGE GRANTS ON ACCOUNT TO ROLE DEMO_ACCESS")
    #sfq.execute("USE ROLE DEMO_ACCESS")
    #sfq.execute("GRANT SELECT ON FUTURE TABLES IN SCHEMA DEMO_DB.TESTANALYTICSDEMO_DB.TESTANALYTICS TO ROLE DEMO_ACCESS")
    sfq.execute("USE DATABASE DEMO_DB")
    sfq.execute("CREATE SCHEMA IF NOT EXISTS TESTANALYTICS")
    sfq.execute("USE WAREHOUSE COMPUTE_WH")
    sfq.execute("USE DATABASE DEMO_DB")
    sfq.execute("USE SCHEMA DEMO_DB.TESTANALYTICS")
    sfq.execute("CREATE OR REPLACE TABLE abc_table(name varchar(20), age integer)")
    #sfq.execute("SHOW PARAMETERS LIKE 'TIMESTAMP_INPUT_FORMAT'")
    #sfq.execute("ALTER SESSION SET TIMESTAMP_INPUT_FORMAT = 'YYYY-MM-DD HH24:MI:SS'")
    sfq.execute("create or replace file format my_csv_format type = csv skip_header = 1 compression = auto")
    sfq.execute("create or replace stage my_stage file_format = my_csv_format;")
    sfq.execute("PUT file://~/SnowFlake_Files/DP-Project/testing/out.csv @~/staged AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
    #sfq.execute("COPY INTO abc_table")
    sfq.execute("copy into abc_table from @~/staged file_format = (format_name = 'my_csv_format') on_error = continue")
    
    print('Steps complete')
    sfq.close()
    sfConnection.close()
    sqq.close()
    sqConnection.close()


#CreateSnowflakeDBandSchema (
#    sfPswd = '',
#    sqPswd = '',
#    sfAccount = 'bqa34388',
#    sfUser = 'USER_1',
#    sfDatabase = 'DEMO_DB',
#    sfSchema = 'TESTANALYTICS',
#    sqUser = 'root',
#    sqHost = 'localhost'
#)
CreateSnowflakeDBandSchema (
    sfPswd = '',
    sqPswd = '',
    sfAccount = 'xu42372.ap-south-1.aws',
    sfUser = 'NEHAMALCOM',
    sfDatabase = 'DEMO_DB',
    sfSchema = 'TESTANALYTICS',
    sqUser = 'root',
    sqHost = 'localhost'
)
