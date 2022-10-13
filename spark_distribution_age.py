import os
import sqlparse

import pandas as pd
import numpy as np

import conn_warehouse   

#function calculate age customer
def get_age(date_start, date_end):
    return date_end.dt.year - date_start.dt.year - ((date_end.dt.month) < (date_start.dt.month)) 

if __name__ == '__main__':
    #connect db warehouse
    conn_dwh, engine_dwh  = conn_warehouse.conn()
    cursor_dwh = conn_dwh.cursor()

    #connect sparks
    conf = conn_warehouse.config('spark')
    spark = conn_warehouse.spark_conn(app="spark_final_project",config=conf)

    #query get db warehouse
    path_query = os.getcwd()+'/queries/'
    query_dwh = sqlparse.format(
       open(
           path_query+'spark_dwh_transaction.sql','r'
           ).read(), strip_comments=True).strip()  

    try :
        print(f"[INFO] Service Spark distribution Running.....")
        #get data from database dwh
        df_dwh = pd.read_sql(query_dwh, engine_dwh)    

        #conver to date    
        df_dwh['birthdate_customer'] = pd.to_datetime(df_dwh['birthdate_customer'])
        df_dwh['date_transaction'] = pd.to_datetime(df_dwh['date_transaction'])
        
        #calculate age
        df_dwh['usia'] = get_age(df_dwh['birthdate_customer'],  df_dwh['date_transaction'])

        #create spark dataframe
        sparkDF = spark.createDataFrame(df_dwh)
        sparkDF.groupBy('usia', 'gender_customer').count().orderBy("usia", ascending=True) \
            .toPandas() \
                .to_csv(f"/mnt/e/digitalskola/linux/final_project/spark_transform/spark_age_output.csv", index=False) 
    
    except :
        print(f"[INFO] Service Spark distribution age Failed .....")



    