#!/usr/bin/python3

import os
import json
import psycopg2

from sqlalchemy import create_engine
from pyspark.sql import SparkSession
from pyspark import SparkContext

def config(param):
    path = os.getcwd()
    with open(path+'/'+'config.json') as file:
        conf = json.load(file)[param]
    return conf

def conn():
    conf = config('warehouse')
    try:
        conn = psycopg2.connect(host=conf['host'], 
                                database=conf['db'], 
                                user=conf['user'], 
                                password=conf['pwd']
                                )
        print(f"[INFO] Success connect Warehouse .....")
        engine = create_engine(f"postgresql+psycopg2://{conf['user']}:{conf['pwd']}@{conf['host']}/{conf['db']}")
        return conn, engine
    except:
        print(f"[INFO] Can't connect Warehouse .....")

def spark_conn(app, config):
    master = config['ip']
    try:
        spark = SparkSession.builder \
            .master(master) \
                .appName(app) \
                    .getOrCreate()
                    
        print(f"[INFO] Success connect SPARK ENGINE .....")
        return spark
    except:
        print(f"[INFO] Success Can't SPARK ENGINE .....")
