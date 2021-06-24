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

    # <--------------------Command Execution-------------------->
    # Getting table company_dim
    sqq.execute("USE qmscore")
    sqq.execute("SELECT * FROM company_dim LIMIT 1000")
    #output = sqq.fetchall()
    #print(output)
    with open("out.csv", "w", newline='') as csv_file: 
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow([i[0] for i in sqq.description]) # write headers
            csv_writer.writerows(sqq)

    

    print('Steps complete')
    sqq.close()
    sqConnection.close()

CreateSnowflakeDBandSchema (
    sfPswd = '',
    sqPswd = '',
    sfAccount = 'xu42372.ap-south-1.aws',
    sfUser = 'NEHAMALCOM',
    sfDatabase = 'DEMO_DB',
    sfSchema = 'TESTANALYTICS',
    sqUser = 'qm_readonly',
    sqHost = 'qmanalyticsdb.cr4wvcewigkc.us-west-2.rds.amazonaws.com'
)
