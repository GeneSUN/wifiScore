from datetime import datetime, timedelta, date
from functools import reduce
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank, sum, concat_ws, col, split, concat_ws, lit ,udf,count, max,lit,avg, when,concat_ws,to_date,explode
from pyspark.sql.functions import col, to_date, month 

import pandas as pd

from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
from pyspark.sql.types import (DateType, DoubleType, StringType, StructType, StructField) 
import sys 
sys.path.append('/usr/apps/vmas/scripts/ZS') 
from MailSender import MailSender

def process_csv_files(date_range, file_path_pattern, dropduplicate=False):  
    """   
    Reads CSV files from HDFS for the given date range and file path pattern and union them.  
    Args:   
        date_range (list): List of date strings in the format 'YYYY-MM-DD'.   
        file_path_pattern (str): File path pattern with a placeholder for the date, e.g., "/user/.../{}.csv"  
        dropduplicate (bool): Whether to drop duplicate rows (default is False).  
    Returns:   
        pyspark.sql.DataFrame: A unioned DataFrame of processed PySpark DataFrames.  
    """   

    def process_csv(date):  
        file_path = file_path_pattern.format(date)
        df_kpis = spark.read.parquet(file_path)  
        if dropduplicate:  
            df_kpis = df_kpis.dropDuplicates()  
        return df_kpis  
    df_list = map(process_csv, date_range)  
    return reduce(lambda df1, df2: df1.union(df2), df_list)

def process_json_files(date_range, file_path_pattern):  
    """   
    Reads CSV files from HDFS for the given date range and file path pattern and union them.  
    Args:   
        date_range (list): List of date strings in the format 'YYYY-MM-DD'.   
        file_path_pattern (str): File path pattern with a placeholder for the date, e.g., "/user/.../{}.csv"  
        dropduplicate (bool): Whether to drop duplicate rows (default is False).  
    Returns:   
        pyspark.sql.DataFrame: A unioned DataFrame of processed PySpark DataFrames.  
    """   

    def process_json(date):  
        file_path = file_path_pattern.format(date)
        try:
            df_kpis = spark.read.json(file_path)  
            return df_kpis
        except Exception as e:  
            print(f"Error reading file for date {date}: {e}")  
    df_list = list(filter(None, map(process_json, date_range)))  
    return reduce(lambda df1, df2: df1.union(df2), df_list)  


if __name__ == "__main__":
    spark = SparkSession.builder\
        .appName('ZheS_device_agg_home')\
        .config("spark.sql.adapative.enabled","true")\
        .getOrCreate()
    mail_sender = MailSender() 
    hdfs_pd = 'hdfs://njbbvmaspd11.nss.vzwnet.com:9000'
    #-----------------------------------------------------------------------
    spark = SparkSession.builder.appName('titan_wifiScore_ZheS').enableHiveSupport().getOrCreate()
    
    start_date = datetime(2024, 1, 1); end_date = datetime(2024, 1, 8) 
    date_list = [ (start_date + timedelta(days=x)).strftime("%Y-%m-%d") for x in range((end_date - start_date).days + 1)] 
    df_list = []
    for date_str in date_list:
        hdfs_title = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/"
        path = hdfs_title + f"/user/maggie/temp1.1/wifiScore_detail{date_str}.json"
        
        try:
            df_temp = spark.read.json(path)
            dfsh_all_Hsc = df_temp.groupby("serial_num","mdn","cust_id")\
                                                .agg(
                                                F.round(sum(df_temp.poor_rssi*df_temp.weights),4).alias("poor_rssi"),\
                                                F.round(sum(df_temp.poor_phyrate*df_temp.weights),4).alias("poor_phyrate"),\
                                                F.round(sum(df_temp.score*df_temp.weights),4).alias("home_score"),\
                                                max("dg_model").alias("dg_model"),\
                                                max("date").alias("date"),\
                                                max("firmware").alias("firmware")
                                                )
            df_vmb = dfsh_all_Hsc.select( "*",F.explode("dg_model").alias("dg_model_indiv") ).dropDuplicates()\
                                    .select("date","serial_num","mdn","cust_id","poor_rssi","poor_phyrate","home_score","dg_model","dg_model_indiv")
            
            try:
                p = f"/usr/apps/vmas/5g_data/fixed_5g_router_mac_sn_mapping/{date_str}/fixed_5g_router_mac_sn_mapping.csv"
                df_join = spark.read.option("header","true").csv(p)\
                                .select( col("mdn_5g").alias("mdn"),
                                        col("serialnumber").alias("serial_num"),
                                        "cust_id"
                                        )
            except:
                p = "/usr/apps/vmas/5g_data/fixed_5g_router_mac_sn_mapping/2023-12-31/fixed_5g_router_mac_sn_mapping.csv"
                df_join = spark.read.option("header","true").csv(p)\
                                .select( col("mdn_5g").alias("mdn"),
                                        col("serialnumber").alias("serial_num"),
                                        "cust_id"
                                        )
                                    
            df1 = df_vmb.drop("mdn","cust_id")
            joined_df = df1.join(df_join,"serial_num", "left").dropDuplicates()
            joined_df.write.mode("overwrite").parquet('/user/ZheS/wifi_score_v2/homeScore_dataframe/'+date_str)
        except Exception as e:
            print(date_str, f"An error occurred: {e}")
