import pyodbc
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s:%(msecs)d %(name)s:%(levelname)s:%(message)s')
file_handler = logging.FileHandler('PervasiveConnectionString.log')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# connect to Pervasive Server
########    10.12.250.46:1583  Latest snapshot IP                        ##########
########    10.12.250.47:1583  USE FOR TESTING, Contains old data        ##########

def connect_to_pervasive(databases, server):
        #call SQL query to get date
    '''enter in databases'''
    try:
        logger.info("connecting to Pervasive Server")
        connect_string = 'DRIVER=Pervasive ODBC Interface;SERVERNAME={server_address};DBQ={db}'
        connections = [pyodbc.connect(connect_string.format(db=n, server_address=server)) for n in databases]

        cursors = [conn.cursor() for conn in connections]
        logger.info("Connection established!")
    except Exception as e:
        logger.critical(f"Error: {e}")
    
    return cursors
