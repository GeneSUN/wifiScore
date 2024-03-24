from pyspark.sql import functions as F 
from pyspark.sql.functions import (collect_list,concat,from_unixtime,lpad, broadcast, sum, udf, col, abs, length, min, max, lit, avg, when, concat_ws, to_date, exp, explode,countDistinct, first,round  ) 
from pyspark.sql import SparkSession 
from pyspark.sql.types import FloatType
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta 
import numpy as np
from dateutil.parser import parse
import argparse 

from functools import reduce 
import sys

def round_numeric_columns(df, decimal_places=2, numeric_columns = None): 

    if numeric_columns == None:
        numeric_columns = [col_name for col_name, data_type in df.dtypes if data_type == "double" or data_type == "float"]

    # Apply rounding to all numeric columns 
    for col_name in numeric_columns: 
        df = df.withColumn(col_name, round(df[col_name], decimal_places)) 
        
    return df
    
def read_parquet_file_by_date(date, file_path_pattern): 

    file_path = file_path_pattern.format(date)
    try: 
        df = spark.read.parquet(file_path)\
                    .withColumn( "Rou_Ext", F.explode( "Rou_Ext" ))\
                    .groupBy("date","serial_num").agg( max("Rou_Ext").alias("Rou_Ext") )
        #print(file_path)
        return df
    except Exception as e:
        print("read_parquet_file_by_date",e,file_path)

def process_parquet_files_for_date_range(date_range, file_path_pattern): 

    df_list = list(map(lambda date: read_parquet_file_by_date(date, file_path_pattern), date_range)) 
    df_list = list(filter(None, df_list)) 
    result_df = reduce(lambda df1, df2: df1.union(df2), df_list) 

    return result_df 
    
class extenders_parquet():
    global hdfs_pd
    hdfs_pd = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/"

    def __init__(self, 
            sparksession,
            install_extender_date,
            window_range,
            device_path,
            home_path
        ) -> None:
        self.spark = sparksession
        self.install_extender_date = install_extender_date # e.g. 10
        self.window_range = window_range # e.g. 30
        self.device_path = device_path
        self.home_path = home_path
        self.before_extender_date = self.install_extender_date - timedelta( self.window_range )
        self.after_extender_date = self.install_extender_date + timedelta( self.window_range + 1 )
        

        self.df_ext_bef_aft = self.get_extender_bef_aft()

    def serialNum_RouExt(self,start_date, window_range = None, device_path = None):
        if window_range is None:
            window_range = self.window_range
        if device_path is None:
            device_path = self.device_path

        date_range = [ ( start_date + timedelta(i) ).strftime('%Y-%m-%d') for i in range(0, window_range)]
        
        # label the device as extender-connected(Rou_Ext = 1) as long as one time connected to the extender
        union_df = process_parquet_files_for_date_range(date_range, device_path)
        result_df = union_df.groupBy("serial_num").agg( F.max("Rou_Ext").alias("Rou_Ext") )
        
        return result_df   

    def get_extender_bef_aft(self, before_extender_date = None, install_extender_date = None, after_extender_date = None, date_window = None):
        
        if before_extender_date is None:
            before_extender_date = self.before_extender_date
        if install_extender_date is None:
            install_extender_date = self.install_extender_date
        if after_extender_date is None:
            after_extender_date = self.after_extender_date
        if date_window is None:
            date_window = self.window_range
        # 1. get extender list monthly ---------------------------------------------------------------
        df1 = self.serialNum_RouExt( start_date = before_extender_date)
        df2 = self.serialNum_RouExt( start_date = install_extender_date, window_range= 1 )
        df3 = self.serialNum_RouExt( start_date = after_extender_date )
        df_extender = df1.filter( col("Rou_Ext") == "0" )\
                        .join( df2.filter( col("Rou_Ext") == "1" ), "serial_num" )\
                        .join( df3.filter( col("Rou_Ext") == "1" ), "serial_num" )\
                        .select("serial_num")
                    
        return df_extender
        
    
if __name__ == "__main__":
    # the only input is the date which is used to generate 'date_range'
    spark = SparkSession.builder.appName('ZheS_wifiscore_get_extender')\
                        .config("spark.sql.adapative.enabled","true")\
                        .getOrCreate()
    hdfs_pd = 'hdfs://njbbvmaspd11.nss.vzwnet.com:9000/'

    #--------------------------------------------------------------------------------
    start_d = date(2024, 2, 21)
    window_range = 7
    #--------------------------------------------------------------------------------
    #for i in range(0,30,window_range):
    d = start_d #+ timedelta(i)

    inst_v3 = extenders_parquet( sparksession = spark, 
                            install_extender_date = d, 
                            window_range = window_range,
                            device_path = hdfs_pd + "/user/ZheS/wifi_score_v3/deviceScore_dataframe/{}",
                            home_path = hdfs_pd + "/user/ZheS/wifi_score_v3/homeScore_dataframe/{}"
                            )

    output_path = (
                hdfs_pd + "/user/ZheS/wifi_score_v3/installExtender_list/" + \
                f"{inst_v3.before_extender_date.strftime('%Y-%m-%d')  }_" +\
                f"{inst_v3.install_extender_date.strftime('%Y-%m-%d')  }_" +\
                f"{inst_v3.after_extender_date.strftime('%Y-%m-%d')  }_" +\
                f"window_range_{inst_v3.window_range}"
                )
    inst_v3.df_ext_bef_aft.write.mode("overwrite").parquet(output_path)


