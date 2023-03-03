import pandas as pd
import logging
from Connections import SQLServerConnect

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s:%(msecs)d %(name)s:%(levelname)s:%(message)s')
file_handler = logging.FileHandler('crud_df_editing.log')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

##
## NOTES: Fixed sitename from dropping! :) 

def clean_up_df(df, sql_table_name, output_file_name, full_server_df, append="append"):
    mhadb = SQLServerConnect(server='W2D-SQLDW01', database='MHADB')
    logger.info("modifying dataframe..")
    try:
        df.replace({False: 0, True: 1}, inplace=True)
        joined_df = df.merge(full_server_df, left_on='StoreNum', right_on='ProdigyStoreNumber')
        joined_df.rename(columns={'ProdigyDatabaseName': 'Site'}, inplace=True)
        joined_df.drop(columns=['ProdigyStoreNumber', 'Filler', 'SiteFinanceKey'], inplace=True)
        column_names = ['Site', 'ProcFlg', 'PharmacyID', 'StoreNum', 'Rx', 'Refill', 'Date', 'Time',
            'TP', 'Tech', 'Fac', 'WritDate', 'MaxFills', 'CostBasis', 'ArType',
            'PriPayType', 'TertPayType', 'SecPayType', 'RecordType', 'Copay',
            'Location', 'Zip', 'Bdate', 'FirstName', 'LastName', 'SvcDate',
            'Relationship', 'Sex', 'Bin', 'Quan', 'Days', 'Ndc', 'DoctId',
            'DoctQual', 'Cost', 'Fee', 'Disc', 'Tax', 'ClaimOut',
            'ClaimIn']
        joined_df = joined_df.reindex(columns=column_names)
        logger.info("Done!")
        logger.info("creating CSV file...")
        print("Saving as CSV & Parquet")
        joined_df.to_csv(f"{output_file_name}.csv", index=False)
        logger.info("CSV file created!")
        logger.info("creating Parquet file...")
        joined_df.to_parquet(f"{output_file_name}.parquet",index=False)
        logger.info("Parquet file created!")
    except Exception as e:
        logger.critical(f"Error modifying df: {e}")

    #### SAVE File to SQL Server using CSV  if in DEV SERVER ######
    try:
        print(f"Writing to SQL Table: {sql_table_name}")
        logger.info(f"Writing to SQL Table: {sql_table_name}")
        mhadb.write_to_sql(output_file_name=output_file_name, tablename=sql_table_name)
        print("Finished writing to SQL")
        logger.info("Finished writing to SQL")
        
        
    except Exception as e:
        logger.critical(f"Error modifying df: {e}") 

    return joined_df
