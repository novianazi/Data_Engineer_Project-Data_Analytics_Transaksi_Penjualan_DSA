#!python3

### memasukan data csv to postgre ###

#library
import pandas as pd
import numpy as np
from sqlalchemy import create_engine


if __name__ == "__main__" :
    #connection postgreSQL
    user_name = 'postgres'
    password = 'admin'
    database = 'digitalskola'
    ip = 'localhost'
    
    try:
        conn = create_engine(f"postgresql://{user_name}:{password}@{ip}:5432/{database}")
        print(f"[INFO] Connection PostgreSQL Success")
    except:
        print(f"[INFO] Connection PostgreSQL Failed") 
    
    #list file csv
    list_filename = ['customer', 'product', 'transaction']
   
   #Dump process file csv
    for file in list_filename :
        pd.read_csv(f"bigdata_{file}.csv").to_sql(f"bigdata_{file}", con=conn, if_exists='replace')
        print(f"[INFO] Dump file {file} Success.....")
        