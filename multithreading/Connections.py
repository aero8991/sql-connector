import pyodbc
from sqlalchemy.engine import URL
from sqlalchemy import create_engine
import pandas as pd
from sqlalchemy import text

## Written by: Dan Rossano
## DAte: 3/3/2023


class SQLServerConnect():

    def __str__(self):
        return 'An object to connect w/ SQL server database'
    
    def __init__(self, server=None, database=None):
        self.drivers = []

        if server is None:
           raise ValueError('Server Name is required!')
       
        if database is None:
           database = 'EDW_Staging'
           print('Database Name should be provided: (EDW_Staging was chosen)!')

        try: 
            assert isinstance(server,str)
            self.server = server
            assert isinstance(database,str)
            self.database = database
        except:
            raise ValueError ('server/database entry must be string')
        
        self.make_SQL_engine()
        self.make_SQL_connection()


    def pyodbc_drivers(self):
        '''
		This function can be used to provide a list of drivers in pyodbc 
		'''
        print ('The following drivers are provided: ')
        
        for driver in pyodbc.drivers():
            self.drivers.append(driver)
            print(str(driver))

##########################################
    def make_SQL_engine(self):
        '''
		Make Engine
		'''
        
        connection_string = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=%s;DATABASE=%s;Trusted_Connection=yes" % (
            self.server,self.database)

        try:
            sqlConnectString =  URL.create(
            "mssql+pyodbc", query={"odbc_connect": connection_string})
            self.connection_engine = create_engine(sqlConnectString)
            
            return self.connection_engine
                
        except Exception as e:
            raise ConnectionError('Failed to create engine server\n'+str(e))
        
      ###########################################################

    def make_SQL_connection(self):
        '''
		Initializing connection to sql server
		'''

        connection_string = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=%s;DATABASE=%s;Trusted_Connection=yes" % (
            self.server,self.database)

        try:
            sqlConnectString =  URL.create(
            "mssql+pyodbc", query={"odbc_connect": connection_string})
            self.connection = create_engine(sqlConnectString).connect()
            

                
        except Exception as e:
            raise ConnectionError('Failed to connect to server\n'+str(e))
        
        self.connection_string = connection_string

        print('Successful Connection')


    def execute_query(self, query, *args, **kwargs):
        '''
        This function is used to run a SQL query and it returns data as a pandas data frame
        '''
        return pd.read_sql(text(query), con=self.connection, **kwargs)

    def write_to_sql(self, output_file_name, tablename):
        if self.server == 'W2D-SQLDW01':
            try:
                df_temp_file = pd.read_csv(f"{output_file_name}.csv", index_col=0)
                df_temp_file.to_sql(tablename,con=self.connection_engine, schema='dbo', if_exists='append' )
                self.connection.close()
        
            except Exception as e:
                print(f"There was an error: {e}")



    def get_pervasive_dbs_from_sql(self):
        '''
        Use to connect to EDW_Staging DB and grabs latest Pervasive 
        ''' 
        sql = '''
         SELECT P.ProdigyDatabaseName, S.SiteFinanceKey, S.ProdigyStoreNumber 
                FROM Prodigy_DLZ.ProdigyDatabases P 
                INNER JOIN MASTERDATA.Sites S 
                ON S.ProdigyDatabaseName = P.ProdigyDatabaseName 
                WHERE ProcessChannelNumber <> 0 
				and S.SiteStatus = 'Active'
				ORDER BY ProdigyDatabaseName;
        '''
        with self.connection as conn:
            result = conn.execute(text(sql))
            df = pd.DataFrame(result.all())
            df = df.dropna()

        self.close()
        return df


    def close(self):
        self.connection.close()



class PervasiveServerConnect():

    def __str__(self):
        return 'An object to connect w/ Pervasive database'
    

    def __init__(self, server=None, database=None, sql=None):
        self.drivers = []

        if server is None:
           server = '10.12.250.46:1583'
           print('Server Name should be provided: (10.12.250.46:1583 was chosen)!')
       
        if sql is None:
            raise ValueError('Pervasive Query is required!')
        
        try: 
            assert isinstance(server,str)
            self.server = server
            self.database = database
            self.sql = sql
        except:
            raise ValueError ('server entry must be string')
        

    
        # self.make_P_connection(database)

    def make_P_connection(self, database):
        '''
		Initializing connection to Pervasive server
		'''
        #server time out currently set to 30 seconds. 
        try:
            connect_string = 'DRIVER=Pervasive ODBC Interface;SERVERNAME={server_address};DBQ={db}'
            self.connection = pyodbc.connect(connect_string.format(db=database, server_address=self.server), timeout=30)
            
        except Exception as e:
            raise ConnectionError('Failed to connect to server\n'+str(e))
        
        print("successful Connection!")
        return self.connection


    def query_pervasive(self, database):
        '''
        This creates connection, cursor, retrieves rows, closes connection, etc. 
        ''' 
        connection = None
        cursor = None
        error_counter = 0
        try:
            connection = self.make_P_connection(database)
            cursor = connection.cursor()
            rows = cursor.execute(self.sql).fetchall()
            return pd.DataFrame.from_records(rows, columns=[col[0] for col in cursor.description])

        #changed Exception (which catches everything) to just except in order to manage error handling.
        except:
            print("There was an error with the connection")
            #trying to reconnect
            if error_counter < 3:
                error_counter += 1
                try:
                    print("Attempting to reconnect...")
                    self.query_pervasive()
                except:
                     raise Exception("Couldnt re-run pervasive query after 3 tries...")
        error_counter = 0

        
    def close(self):
        '''
        Use to close the pervasive Connection
        '''
        self.connection.close()
