from Connections import SQLServerConnect, PervasiveServerConnect
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from crud import clean_up_df
import random 



start_time = datetime.now()

N_DAYS_AGO = 3

n_days_ago = start_time - timedelta(days=N_DAYS_AGO)
start_date = start_time.strftime("%Y%m%d")  #Today's date as a string
end_date = n_days_ago.strftime("%Y%m%d")  ## n days ago as a string
test_date = '20210101'
start = '20230110'
end = '20230112'

sql = f"SELECT top 10000 * FROM MHA where Date between {start} and {end}"
sql2 = f"SELECT COUNT(*)  FROM MHA where Date between {start} and {end};" 

pervasive = PervasiveServerConnect(sql=sql)
record_count = PervasiveServerConnect(sql=sql2)
#df = pervasive.query_pervasive(sql)

#print(df)

# cursor = pervasive.cursor()
# rows = pervasive.execute(sql).fetchall()
# df = pd.DataFrame.from_records(rows, columns=[col[0] for col in cursor.description])
# print(df)


databases = SQLServerConnect(server='W2D-SQLDW01')
df = databases.get_pervasive_dbs_from_sql()
full_server_list = df['ProdigyDatabaseName'].dropna().to_list()


one_hundred = random.sample(full_server_list, 100)
print(len(one_hundred))




with ThreadPoolExecutor(len(one_hundred)) as executor:
    data = list(executor.map(pervasive.query_pervasive, one_hundred))
    countinfo = list(executor.map(record_count.query_pervasive, one_hundred))

try:
    joined = pd.concat(data).reset_index(drop=True)
    #print(joined)

    #add list to dataframe
    count_info = pd.concat(countinfo).reset_index(drop=True)
    count_info['sites'] = one_hundred
    count_info.to_csv('MHA_row_counts.csv')
except Exception as e:
    print(f"Error: {e}")

sql_table_name = 'MHA_clone'
filename = 'Outputfile_MHA'



file_as_df = clean_up_df(
    df=joined,
    output_file_name=filename,
    sql_table_name=sql_table_name,
    full_server_df=df
)

elapsed = datetime.now() - start_time
print(f'Execution time to complete: {elapsed}')
