from Loading_task_1_updated import snowflakeConnector
import json

def configure():
    with open('config.json') as config_file:
        data = json.load(config_file)
    global sfUser
    global sfPswd
    global sfAccount
    global sfRole
    global sfDatabase
    global sfSchema
    global sfWarehouse
    sfUser = data['SNOWFLAKE']['USERNAME']
    sfPswd = data['SNOWFLAKE']['PASSWORD']
    sfAccount = data['SNOWFLAKE']['ACCOUNT']
    sfRole = data['SNOWFLAKE']['ROLE']
    sfDatabase = data['SNOWFLAKE']['DATABASE']
    sfSchema = data['SNOWFLAKE']['SCHEMA']
    sfWarehouse = data['SNOWFLAKE']['WAREHOUSE']
    sfQueryFile = data['QUERY']['SFQUERIES']

if __name__=="__main__":
    # getting required configuration settings from config.json
    configure()
    # function to load data from MySQL server to Snowflake
    sfConnection, sfq = snowflakeConnector (
        sfUser = sfUser,
        sfPswd = sfPswd,
        sfAccount = sfAccount,
        sfDatabase = sfDatabase,
        sfSchema = sfSchema
    )
    sfq.execute(f"USE ROLE {sfRole}")
    sfq.execute(f"USE WAREHOUSE {sfWarehouse}")
    sfq.execute(f"USE DATABASE {sfDatabase}")
    sfq.execute(f"USE SCHEMA {sfDatabase}.{sfSchema}")

    f = open(sfQueryFile, 'r')
    sqlFile = f.read()
    f.close()
    sqlCommands = sqlFile.split(';')

    for command in sqlCommands:
        try:
            sfq.execute(command)
        except OperationalError, msg:
            print(f"Command {command}skipped", msg)


    sfq.close()
    sfConnection.close()
