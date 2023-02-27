from sqlalchemy.engine import URL
import pandas as pd
from sqlalchemy import text
import logging
import numpy as np

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s:%(msecs)d %(name)s:%(levelname)s:%(message)s')
file_handler = logging.FileHandler('SQLConnectionString.log')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

#connections
## W2D-SQLDW01 closes after 6:30 PM and is offline over the weekend

def create_server_connection(server, database):
    try:
        logger.info("Trying to connect to SQL Server...")
        connection_string = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=%s;DATABASE=%s;Trusted_Connection=yes" % (
            server, database)

        connection_url = URL.create(
            "mssql+pyodbc", query={"odbc_connect": connection_string})
        print(f"MySQL Database connection to {database} successful!")
        logger.info(f"MySQL Database connection to {database} successful!")
        
    except Exception as e:
        print(f"Error:  {e}")
        logger.critical(f"Error:  {e}")

    return connection_url

###
#Converts store's w/o a defined number to -1 until properly defined by the MDS process
# pervasive_sites() uses the EDW DB connection 
###
def pervasive_sites(runner):
    logger.info("Searching DimSite Table for Prodigy DBs")
    logger.warn("Check how many times run.")

    sql = '''SELECT distinct ProdigyReportName, ProdigyStoreNumber, ProdigyDatabaseName
            FROM [EDW].[dbo].[DimSite]
            where ISNULL(ProdigyReportName,'') <> ''
            ORDER BY ProdigyStoreNumber
            '''
    logger.info("Converting data from SQL into a DF")
    with runner.connect() as conn:
        
        result = conn.execute(text(sql))
        df = pd.DataFrame(result.all())
        df['ProdigyStoreNumber'] = df['ProdigyStoreNumber'].fillna(-1)
        df['ProdigyStoreNumber'] = df['ProdigyStoreNumber'].astype(int)
        df.loc[df.ProdigyDatabaseName == '',  'ProdigyDatabaseName'] = np.nan
        
        
    conn.close()
    logger.info("Data pulled and now in a dataframe!")
    return df
