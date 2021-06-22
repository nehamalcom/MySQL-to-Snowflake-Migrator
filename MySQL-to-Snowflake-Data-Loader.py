def CreateSnowflakeDBandSchema (
    sfPswd = '',
    sqPswd = '',
    sfAccount = 'myAccount.my-region',
    sfUser = 'my_user',
    sfDatabase = 'DEMO_DB',
    sfSchema = 'DEMO_SCHEMA'
):
    # Snowflake connection setup
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






    # MySQL connection setup
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
          user='root',
          password=sqPswd,
          host='localhost'
      )
    sqq = sfConnection.cursor()
    
    import csv
    # DATABASE CREATION IN MYSQL
    #sqq.execute("CREATE DATABASE abc_db ")
    #print ("MySQL: Database abc_db created.")
    sqq.execute("USE abc_db ")
    #sqq.execute("CREATE TABLE abc_table (name varchar(20), age integer)")
    #print("MySQL: Table created.")
    #sqq.execute("INSERT INTO abc_table (name, age) VALUES ('Neha', 20)")
    #sqq.execute("INSERT INTO abc_table (name, age) VALUES ('Raymol', 47)")
    #sqq.execute("INSERT INTO abc_table (name, age) VALUES ('Malcom', 50)")
    #sqq.execute("DROP DATABASE abc_db ")
    #print ("MySQL: Database abc_db dropped.")
    sqq.execute("SHOW DATABASES ")
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
    sqq.execute("SELECT * FROM abc_table")
    with open("out.csv", "w", newline='') as csv_file: 
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow([i[0] for i in sqq.description]) # write headers
            csv_writer.writerows(sqq)
    
    
    # LOADING CSV INTO SNOWFLAKE
    sfq.execute("CREATE WAREHOUSE IF NOT EXISTS abc_wh")
    sfq.execute("CREATE DATABASE IF NOT EXISTS abc_db")
    sfq.execute("USE DATABASE abc_db")
    sfq.execute("CREATE SCHEMA IF NOT EXISTS abc_schema")
    sfq.execute("USE WAREHOUSE abc_wh")
    sfq.execute("USE DATABASE abc_db")
    sfq.execute("USE SCHEMA abc_db.abc_schema")
    sfq.execute("CREATE OR REPLACE TABLE abc_table(name varchar(20), age integer)")
    #sfq.execute("SHOW PARAMETERS LIKE 'TIMESTAMP_INPUT_FORMAT'")
    #sfq.execute("ALTER SESSION SET TIMESTAMP_INPUT_FORMAT = 'YYYY-MM-DD HH24:MI:SS'")
    sfq.execute("create or replace file format my_csv_format type = csv skip_header = 1 compression = auto")
    sfq.execute("create or replace stage my_stage file_format = my_csv_format;")
    sfq.execute("PUT file://~/SnowFlake_Files/DP-Project/out.csv @~/staged AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
    #sfq.execute("COPY INTO abc_table")
    sfq.execute("copy into abc_table from @~/staged file_format = (format_name = 'my_csv_format') on_error = continue")
    
    print('Steps complete')
    sfq.close()
    sfConnection.close()
    sqq.close()
    sqConnection.close()
