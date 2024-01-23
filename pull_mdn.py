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


if __name__ == "__main__":
    spark = SparkSession.builder\
        .appName('ZheS_monthly_agg')\
        .config("spark.sql.adapative.enabled","true")\
        .getOrCreate()
    mail_sender = MailSender() 
    hdfs_pd = 'hdfs://njbbvmaspd11.nss.vzwnet.com:9000'
    mdn_list = ["6099170411",
                "5013263000",
                "5014491454",
                "3854471350",
                "7708517580",
                "3852263274",
                "9082976601"]
    #----------------------------------------------------------------------
    def process_file(date, file_path_pattern=hdfs_pd + "/user/ZheS/wifi_score_v2/homeScore_dataframe/{}"):  
        file_path = file_path_pattern.format(date)
        try:
            df_kpis = spark.read.parquet(file_path).select("serial_num","home_score","dg_model_indiv","mdn","cust_id","date")
            return df_kpis
        except Exception as e:
            print(e)
    
    start_date = datetime(2023, 12, 1); end_date = datetime(2024, 1, 22) 
    date_list = [ (start_date + timedelta(days=x)).strftime("%Y-%m-%d") for x in range((end_date - start_date).days + 1)] 
    file_path_pattern = hdfs_pd + "/user/ZheS/wifi_score_v2/homeScore_dataframe/{}"

    df_list = list(filter(None, map(process_file, date_list)))  
    df_union = reduce(lambda df1, df2: df1.union(df2), df_list)

    df_send = df_union.filter( col("mdn").isin(mdn_list))

    pd_df = df_send.toPandas()
    report_file_name = f'{date_list[-1]}_{date_list[0]}.xlsx' 
    writer = pd.ExcelWriter(report_file_name) 
    pd_df.to_excel(writer) 
    writer.close() 
    mail_sender.send( files=[report_file_name], subject = "monthly_summary" ) 