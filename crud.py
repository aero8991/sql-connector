import pandas as pd
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s:%(msecs)d %(name)s:%(levelname)s:%(message)s')
file_handler = logging.FileHandler('crud_df_editing.log')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


def clean_up_df(df, sql_table_name, output_file_name, full_server_df, engine, server, append="append"):
    logger.info("modifying dataframe..")
    try:
        df.replace({False: 0, True: 1}, inplace=True)
        joined_df = df.merge(full_server_df, left_on='StoreNum', right_on='ProdigyStoreNumber')
        joined_df.rename(columns={'ProdigyReportName': 'Site'}, inplace=True)
        joined_df.drop(columns=['ProdigyStoreNumber', 'Filler'], inplace=True)
        column_names = ['Site', 'ProcFlg', 'PharmacyID', 'StoreNum', 'Rx', 'Refill', 'Date', 'Time',
            'TP', 'Tech', 'Fac', 'WritDate', 'MaxFills', 'CostBasis', 'ArType',
            'PriPayType', 'TertPayType', 'SecPayType', 'RecordType', 'Copay',
            'Location', 'Zip', 'Bdate', 'FirstName', 'LastName', 'SvcDate',
            'Relationship', 'Sex', 'Bin', 'Quan', 'Days', 'Ndc', 'DoctId',
            'DoctQual', 'Cost', 'Fee', 'Disc', 'Tax', 'ClaimOut',
            'ClaimIn']
        joined_df = joined_df.reindex(columns=column_names)
        logger.info("creating CSV file...")
        joined_df.to_csv(f"{output_file_name}.csv", index=False)
        logger.info("CSV file created!")
    except Exception as e:
        logger.critical(f"Error modifying df: {e}")

    #### SAVE File to SQL Server using CSV  if in DEV SERVER ######
    if server == 'W2D-SQLDW01':
        try:    
            df_temp_file = pd.read_csv(f"{output_file_name}.csv", index_col=0)
            print("transferring data to SQL...")
            logger.info("transferring data to SQL...")
            df_temp_file.to_sql(sql_table_name, con=engine, schema='dbo', if_exists=append)
            print(f"data transferred to {sql_table_name} table successfully!")
            logger.info(f"data transferred to {sql_table_name} table successfully!")
        except Exception as e:
            print(f"There was an error: {e}")
            logger.info(f"There was an error: {e}")
    ### SAVES file to Parquet file ###    
    logger.info("creating Parquet file...")
    joined_df.to_parquet(f"{output_file_name}.parquet",index=False)
    logger.info("Parquet file created!")
    return joined_df
