import os
import sqlparse

import pandas as pd
import numpy as np

import conn_warehouse   


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
        print(f"[INFO] Service Spark Summary Product Running.....")
        #get data from database dwh
        df_dwh = pd.read_sql(query_dwh, engine_dwh)    
        #saprk dataframe
        sparkDF = spark.createDataFrame(df_dwh)

        #spark processing Product
        sparkDF.groupBy("product_transaction").sum("amount_transaction") \
            .toPandas() \
                .to_csv(f"/mnt/e/digitalskola/linux/final_project/spark_transform/spark_product_output.csv", index=False) 

        
    except :
        print(f"[INFO] Service Spark Summary Product Failed .....")


    