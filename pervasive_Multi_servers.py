import pandas as pd
from sqlalchemy import create_engine
from Pervasive_multi_connect import connect_to_pervasive
from crud import clean_up_df
from PervasiveDatabases import pervasive_sites, create_server_connection
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s:%(msecs)d %(name)s:%(levelname)s:%(message)s')
file_handler = logging.FileHandler('main_connection_file_pervasive.log')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

start_time = datetime.now()
database = 'EDW'
database_MHA = 'MHADB'


##### MAIN FILE TO RUN  ########################################################################################
###   Step 1: Determine which SQL server to run on. 'W2P-SQLDW01' or 'W2D-SQLDW01'   **** W2D-SQLDW01 closes after 6:30 PM PST and is offline over the weekend ***
###   Step 2: Choose a list of databases from pervasive to search from
###   Step 3: Choose the Pervasive server you want to connect with.  Enter choices into the Cursors variable
###   Step 4: Modify your SQL query            
###   Step 5: Modify which SQL table to store data on. If using prod then this is skipped    ***Table needs to be built/formatted exactly like the MHA table before running ***
###   Step 6: Modify file name for .CSV file and .Parquet output
###   Step 7: Run script! :) 

#### FUTURE STATE ###################################################################################
##  **modify N-Days to be 1 day behind to get a 1 day lookback of the servers. 
### **parquet files can be placed in AWS S3 bucket. I Recommend using the python boto3 library to programatically transfer parquet file to S3 bucket.
##  **Boto3 section can be added to the end of this script to complete pipeline end-to-end process
##  ** create an AWS glue crawler to scan the .parquet files and set it to pick up only the changes to the file. 



#### Prod Vs Dev SQL server ###
## W2D-SQLDW01 disconnects after 6:30 PM and is offline over the weekend

#server = 'W2P-SQLDW01'
server = 'W2D-SQLDW01'

#EDW SQL Connection
sql_server = create_server_connection(server=server, database=database)
runner = create_engine(sql_server)
server_list = pervasive_sites(runner)

#MHADB DEV SQL Connection
#currently MHADB only exists on DEV Server
sql_server_MHA = create_server_connection(server=server, database=database_MHA)
engine_MHA = create_engine(sql_server_MHA)



# list for connecting w/ multiple servers
#full list of DBs - ALL DBS #### WARNING THIS WILL SLOW PERFORMANCE 
full_list_of_dbs = server_list['ProdigyDatabaseName'].dropna().to_list()
print(full_list_of_dbs)
#test DBs
databases = ['ANDERSON', 'AUBURN', 'BATTLECREEK', 'MADISON', 'MUNCIE', 'NEWBRIGHTON', 'PROVIDENCE', 'ROSEVILLE']
short_databases = ['ANDERSON', 'AUBURN', 'BATTLECREEK']
very_short_database = ['ANDERSON']

#### CHOOSE A DATABASE AND SERVER TO PASS #####

########    10.12.250.46:1583  Latest snapshot IP                        ##########
########    10.12.250.47:1583  USE FOR TESTING, Contains old data        ##########

snapshot_server_ip = '10.12.250.46:1583'
test_server_ip = '10.12.250.47:1583'


### ENTER choices here ###################
connections= connect_to_pervasive(databases=full_list_of_dbs, server=snapshot_server_ip)
logger.info(f"length of dbs passed to Pervasive Server: {len(connections)}")


#### MODIFY SQL QUERIES #########################################################

#n days ago from today's date
N_DAYS_AGO = 22

n_days_ago = start_time - timedelta(days=N_DAYS_AGO)
start_date = start_time.strftime("%Y%m%d")  #Today's date as a string
end_date = n_days_ago.strftime("%Y%m%d")  ## n days ago as a string
test_date = '20210101'

#use sql_testing for 10.12.250.47:1583 Server
#sql_testing = f"SELECT top 10 * FROM MHA WHERE Date > '{test_date}'"

#use sql for 10.12.250.46:1583

sql = f"SELECT * FROM MHA where Date between {end_date} and {start_date}"

#sql = f"SELECT top 10 * FROM MHA WHERE Date > {test_date}"

#data pull from servers
data = []
try:
    for cur in connections:
        rows = cur.execute(sql).fetchall()
        df = pd.DataFrame.from_records(rows, columns=[col[0] for col in cur.description])
        data.append(df)

except Exception as e:
    print(e)
    logger.critical(f'Error: {e}')

finally:
    for cur in connections:
        cur.close()

df = pd.concat(data, axis=0).reset_index(drop=True)


######location to store the data in SQL #######################
sql_table_name = 'MHA_clone'
filename = 'Outputfile_MHA'

file_as_df = clean_up_df(
    df=df,
    output_file_name=filename,
    sql_table_name=sql_table_name,
    full_server_df=server_list,
    engine=engine_MHA,
    server=server,
)

elapsed = datetime.now() - start_time
print(f'Execution time to complete: {elapsed}')
logger.info(f'Execution time to complete: {elapsed}')
