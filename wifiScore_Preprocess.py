from pyspark.sql import functions as F 
from pyspark.sql.functions import (concat,from_unixtime,lpad, broadcast, sum, udf, col, abs, length, min, max, lit, avg, when, concat_ws, to_date, exp, explode,countDistinct, first,round  ) 
from pyspark.sql import SparkSession 
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta 

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

def get_monthly_extender(start_date, window_range = 30):
    df_list = []
    
    # label the device as extender-connected(Rou_Ext = 1) as long as one time connected to the extender
    for i in range(1, window_range):
        d = ( start_date + timedelta(i) ).strftime('%Y-%m-%d')
        try:
            df = spark.read.json(hdfs_pd + f"user/maggie/temp1.1/wifiScore_detail{d}.json")\
                        .withColumn( "Rou_Ext", F.explode( "Rou_Ext" ))\
                        .groupBy("date","serial_num").agg( max("Rou_Ext").alias("Rou_Ext") )
            df_list.append(df)
        except Exception as e:
            print(e)
            print(d)
    # label the home as extender-connected(Rou_Ext = 1) as long as one device connected to the extender
    union_df = reduce(lambda df1, df2: df1.unionAll(df2), df_list) 
    result_df = union_df.groupBy("serial_num").agg( F.max("Rou_Ext").alias("Rou_Ext") )
    
    return result_df

def month_agg_serial(date_range, df_extender, file_path_pattern = None, columns_to_agg = None,  id_columns = None):
    if file_path_pattern is None:
        file_path_pattern = 'hdfs://njbbvmaspd11.nss.vzwnet.com:9000/' + "user/maggie/temp1.1/wifiScore_detail{}.json"
    if columns_to_agg is None:
        columns_to_agg = ["avg_phyrate", "poor_phyrate", "poor_rssi", "score", "weights", 'count_rssi_drop', 'count_rssi_ori','stationarity'] 
    if id_columns is None:
        id_columns = ['#dropped', 'Rou_Ext', 'avg_phyrate', 'count_rssi_drop', 'count_rssi_ori', 'cust_id', 'date', 'dg_model', 'firmware', 'mdn', 'poor_phyrate', 'poor_rssi', 'rk_row_sn', 'rowkey', 'score', 'serial_num', 'station_mac', 'stationarity', 'weights']
    
    df_list = [] 
    for d in date_range: 
        file_path = file_path_pattern.format( d )
        try:
            df_kpis = spark.read.json(file_path).select(id_columns)
            
            df_list.append(df_kpis)
        except Exception as e:
            print(e)    

    Extenders = reduce(lambda df1, df2: df1.unionAll(df2), df_list).join( broadcast(df_extender), "serial_num" )
    average_columns = [F.avg(col).alias(col) for col in columns_to_agg]
    median_columns = [ F.expr(f"percentile_approx({col}, {0.5})").alias(f"median_{col}") 
                        for col in columns_to_agg] 

    result_df = Extenders.groupBy("serial_num")\
                                .agg(*average_columns, 
                                        *median_columns, 
                                        F.count("*").alias("count")) 

    numeric_columns = [e for e in result_df.columns if e != "serial_num"]
    return round_numeric_columns(result_df, decimal_places= 4, numeric_columns = numeric_columns )

def mergeHomeData(extender_month,date_window = 30):

    install_extender_date = datetime(2023, extender_month, 1); 
    before_extender_date = install_extender_date - relativedelta(months=1); 
    after_extender_date = install_extender_date + relativedelta(months=1) 

    # 1. get extender list monthly ---------------------------------------------------------------
    df1 = get_monthly_extender( start_date = before_extender_date )
    df2 = get_monthly_extender( start_date = install_extender_date )
    df3 = get_monthly_extender( start_date = install_extender_date )
    df_extender = df1.filter( col("Rou_Ext") == "0" )\
                    .join( df2.filter( col("Rou_Ext") == "1" ), "serial_num" )\
                    .join( df3.filter( col("Rou_Ext") == "1" ), "serial_num" )\
                    .select("serial_num")

    # 2. join before extender features and after extender features (exclude install extender month)
    
    after_range = [ ( after_extender_date + timedelta(i) ).strftime('%Y-%m-%d') for i in range(date_window) ]
    after_serial_features = month_agg_serial(after_range, df_extender)\
                            .select( "serial_num", col("score").alias("target_score") )
    
    before_range = [ ( before_extender_date + timedelta(i) ).strftime('%Y-%m-%d') for i in range(date_window) ]
    before_serial_features = month_agg_serial(before_range,  df_extender)
    #before_serial_features.show(); sys.exit()
    output_path = hdfs_pd + "/user/ZheS/wifi_score_v2/training_dataset/" + \
                        f"{before_extender_date.strftime('%b')}_{install_extender_date.strftime('%b')}_{after_extender_date.strftime('%b')}"

    before_serial_features.join(after_serial_features, "serial_num" )\
                        .repartition(10)\
                        .write.mode("overwrite").parquet(output_path)
    
if __name__ == "__main__":
    # the only input is the date which is used to generate 'date_range'
    spark = SparkSession.builder.appName('ZheS_wifiscore')\
                        .config("spark.sql.adapative.enabled","true")\
                        .getOrCreate()
    hdfs_pd = 'hdfs://njbbvmaspd11.nss.vzwnet.com:9000/'

    #--------------------------------------------------------------------------------
    extender_month = 10
    #--------------------------------------------------------------------------------
    mergeHomeData( extender_month )







