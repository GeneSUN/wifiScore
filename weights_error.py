from datetime import datetime, timedelta, date
from functools import reduce
from pyspark.sql.window import Window
from pyspark.sql.functions import broadcast, row_number, rank, dense_rank, sum, concat_ws, col, split, concat_ws, lit ,udf,count, max,lit,avg, when,concat_ws,to_date,explode
from pyspark.sql.functions import col, to_date, month 
from pyspark.sql import functions as F
import pandas as pd

from pyspark.sql.types import *
from pyspark.sql import SparkSession

import os
from pyspark.sql.types import (DateType, DoubleType, StringType, StructType, StructField) 
import sys 
sys.path.append('/usr/apps/vmas/scripts/ZS') 
from MailSender import MailSender

from math import radians, cos, sin, asin, sqrt
import concurrent.futures 
def read_csv_file_by_date(date, file_path_pattern): 

    file_path = file_path_pattern.format(date) 
    df = spark.read.option("recursiveFileLookup", "true").option("header", "true").csv(file_path)

    return df 

def process_csv_files_for_date_range(date_range, file_path_pattern): 

    df_list = list(map(lambda date: read_csv_file_by_date(date, file_path_pattern), date_range)) 
    #df_list = list(filter(None, df_list)) 
    result_df = reduce(lambda df1, df2: df1.union(df2), df_list) 

    return result_df 

def convert_string_numerical(df, String_typeCols_List): 
    """ 
    This function takes a PySpark DataFrame and a list of column names specified in 'String_typeCols_List'. 
    It casts the columns in the DataFrame to double type if they are not in the list, leaving other columns 
    as they are. 

    Parameters: 
    - df (DataFrame): The PySpark DataFrame to be processed. 
    - String_typeCols_List (list): A list of column names not to be cast to double. 

    Returns: 
    - DataFrame: A PySpark DataFrame with selected columns cast to double. 
    """ 
    # Cast selected columns to double, leaving others as they are 
    df = df.select([F.col(column).cast('double') if column not in String_typeCols_List else F.col(column) for column in df.columns]) 
    return df

if __name__ == "__main__":
    hdfs_pd = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/"
    spark = SparkSession.builder\
        .appName('ZheS_wifiscore_weight_normalization')\
        .config("spark.sql.adapative.enabled","true")\
        .getOrCreate()
    
    for i in range(21):
        day = date(2024,1,8)+ timedelta(i)
        date_str1 = day.strftime("%Y%m%d") # e.g. 20231223
        date_str2 = day.strftime("%Y-%m-%d") # e.g. 2023-12-23

        window_spec = Window().partitionBy("serial_num") 
                
        df_deviceScore = spark.read.parquet("/user/ZheS/wifi_score_v2/deviceScore_dataframe/"+date_str2 )\
                .groupby("serial_num","mdn","cust_id","station_mac","poor_rssi","poor_phyrate","weights","device_score")\
                .agg(
                    F.collect_set("dg_model").alias("dg_model"), 
                    F.collect_set("Rou_Ext").alias("Rou_Ext")
                )
        
        df_homeScore = df_deviceScore.groupBy("serial_num", "mdn", "cust_id")\
                    .agg(  
                        F.round(F.sum(col("poor_rssi") * col("weights")), 4).alias("poor_rssi"),  
                        F.round(F.sum(col("poor_phyrate") * col("weights")), 4).alias("poor_phyrate"),  
                        F.round(F.sum(col("device_score") * col("weights")), 4).alias("home_score"), 
                        F.collect_set("dg_model").alias("dg_model"), 
                        F.collect_set("Rou_Ext").alias("Rou_Ext"), 
                    )\
                    .select( "*",F.explode("dg_model").alias("dg_model_mid1") ).dropDuplicates()\
                    .select( "*",F.explode("dg_model_mid1").alias("dg_model_mid2") )\
                    .select( "*",F.explode("dg_model_mid2").alias("dg_model_indiv") )\
                    .drop("dg_model_mid1","dg_model_mid2")\
                    .dropDuplicates()

        df_homeScore.write.mode("overwrite")\
                    .parquet( hdfs_pd + "/user/ZheS/wifi_score_v1/homeScore_dataframe/" + date_str2 )
                
