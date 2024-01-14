from pyspark.sql import functions as F 
from pyspark.sql.functions import (concat,from_unixtime,lpad, broadcast, sum, udf, col, abs, length, min, max, lit, avg, when, concat_ws, to_date, exp, explode,countDistinct, first,round  ) 
from pyspark.sql import SparkSession 
from datetime import datetime, timedelta, date 
from dateutil.parser import parse
import argparse 
import pandas as pd
import smtplib
import smtplib
from email.mime.text import MIMEText
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.utils import COMMASPACE, formatdate
import os

def round_numeric_columns(df, decimal_places=2, numeric_columns = None): 

    if numeric_columns == None:
        numeric_columns = [col_name for col_name, data_type in df.dtypes if data_type == "double" or data_type == "float"]

    # Apply rounding to all numeric columns 
    for col_name in numeric_columns: 
        df = df.withColumn(col_name, round(df[col_name], decimal_places)) 
        
    return df

def union_df_list(df_list):   

    """   
    Union a list of DataFrames and apply filters to remove duplicates and null values in 'ENODEB' column.   
    Args:   
        df_list (list): List of PySpark DataFrames to be unioned.  
    Returns:  
        DataFrame: Unioned DataFrame with duplicates removed and filters applied.   

    """   
    # Initialize the result DataFrame with the first DataFrame in the list
    try:
        df_post = df_list[0]
    except Exception as e:    
        # Handle the case where data is missing for the current DataFrame (df_temp_kpi)   
        print(f"Error processing DataFrame {0}: {e}")   
        
    # Iterate through the rest of the DataFrames in the list   
    for i in range(1, len(df_list)):    
        try:    
            # Get the current DataFrame from the list   
            df_temp_kpi = df_list[i]   
            # Union the data from df_temp_kpi with df_kpis and apply filters    
            df_post = (   
                df_post.union(df_temp_kpi)   
            )   
        except Exception as e:    
            # Handle the case where data is missing for the current DataFrame (df_temp_kpi)   
            print(f"Error processing DataFrame {i}: {e}")   
            # Optionally, you can log the error for further investigation   
    return df_post

if __name__ == "__main__":
    # the only input is the date which is used to generate 'date_range'
    spark = SparkSession.builder.appName('ZheS_wifiscore')\
                        .config("spark.sql.adapative.enabled","true")\
                        .getOrCreate()
    hdfs_pd = 'hdfs://njbbvmaspd11.nss.vzwnet.com:9000/'
    id_columns = ['#dropped', 'Rou_Ext', 'avg_phyrate', 'count_rssi_drop', 'count_rssi_ori', 'cust_id', 'date', 'dg_model', 'firmware', 'mdn', 'poor_phyrate', 'poor_rssi', 'rk_row_sn', 'rowkey', 'score', 'serial_num', 'station_mac', 'stationarity', 'weights']
    
    df_aug = spark.read.parquet(hdfs_pd+"/user/ZheS/wifi_score_v2/Rou_ext_Aug")\
                    .groupBy("serial_num").agg( F.max("Rou_Ext").alias("Rou_Ext") )
    df_sept = spark.read.parquet(hdfs_pd+"/user/ZheS/wifi_score_v2/Rou_ext_Sept")\
                    .groupBy("serial_num").agg( F.max("Rou_Ext").alias("Rou_Ext") )
    df_join = df_aug.filter( col("Rou_Ext") == "0" ).join( df_sept.filter( col("Rou_Ext") == "1" ), "serial_num" )\
                    .select("serial_num")



    file_path_pattern = hdfs_pd + "user/maggie/temp1.1/wifiScore_detail{}.json"
    id_columns = ['#dropped', 'Rou_Ext', 'avg_phyrate', 'count_rssi_drop', 'count_rssi_ori', 'cust_id', 'date', 'dg_model', 'firmware', 'mdn', 'poor_phyrate', 'poor_rssi', 'rk_row_sn', 'rowkey', 'score', 'serial_num', 'station_mac', 'stationarity', 'weights']
    columns_to_average = ["avg_phyrate", "poor_phyrate", "poor_rssi", "score", "weights", 'count_rssi_drop', 'count_rssi_ori','stationarity'] 

    def cal_avg_serial(date_range,file_path_pattern, id_columns, columns_to_average):
        df_list = [] 
        for d in date_range: 
            file_path = file_path_pattern.format( d )
            try:
                df_kpis = spark.read.json(file_path).select(id_columns)
                
                df_list.append(df_kpis)
            except Exception as e:
                print(e)    

        aug_noExtenders = union_df_list(df_list).join( df_join, "serial_num" )
        average_columns = [F.avg(col).alias(col) for col in columns_to_average]
        median_columns = [ F.expr(f"percentile_approx({col}, {0.5})").alias(f"median_{col}") 
            for col in columns_to_average] 



        result_df = aug_noExtenders.groupBy("serial_num")\
                                    .agg(*average_columns, 
                                         *median_columns, 
                                         F.count("*").alias("count")) 
        #averages = {col_name: "avg" for col_name in columns_to_average} 
        #aug_noExtenders.groupby("serial_num").agg(averages)
        numeric_columns = [e for e in result_df.columns if e != "serial_num"]
        return round_numeric_columns(result_df, decimal_places= 4, numeric_columns = numeric_columns )
    
    """
    
    oct_range = [ (datetime(2023, 10 , 1) + timedelta(i) ).strftime('%Y-%m-%d') for i in range(30) ]
    oct_serial_features = cal_avg_serial(oct_range,file_path_pattern, id_columns,columns_to_average)\
                            .select( "serial_num", col("score").alias("target_score") )
    
    aug_range = [ (datetime(2023, 8 , 1) + timedelta(i) ).strftime('%Y-%m-%d') for i in range(30) ]
    aug_serial_features = cal_avg_serial(aug_range, file_path_pattern, id_columns,columns_to_average)

    output_path = hdfs_pd + "/user/ZheS/wifi_score_v2/training_data"
    aug_serial_features.join(oct_serial_features, "serial_num" )\
                        .coalesce(10)\
                        .write.mode("overwrite").parquet(output_path)
    #            .repartition(1).write.csv(output_path, header=True, mode="overwrite") 

    """



    def cal_avg(date_range,file_path_pattern, id_columns):
        df_list = [] 
        for d in date_range: 
            file_path = file_path_pattern.format( d )
            try:
                df_kpis = spark.read.json(file_path).select(id_columns)
                
                df_list.append(df_kpis)
            except Exception as e:
                print(e)    
        aug_all =  union_df_list(df_list)
        aug_noExtenders = aug_all.join( df_join, "serial_num" )

        # Columns for which you want to calculate the average 
        
        averages = {col_name: "avg" for col_name in columns_to_average} 
        print( date_range[0][:7], "only extender experiment group" )
        aug_noExtenders.agg(averages).show()
        print( date_range[0][:7], "all group" )
        aug_all.agg(averages).show()

    """
    cal_avg([ (datetime(2023, 8 , 1) + timedelta(i) ).strftime('%Y-%m-%d') for i in range(30) ],
          file_path_pattern, id_columns)
    cal_avg([ (datetime(2023, 9 , 1) + timedelta(i) ).strftime('%Y-%m-%d') for i in range(30) ],
          file_path_pattern, id_columns)
    cal_avg([ (datetime(2023, 10 , 1) + timedelta(i) ).strftime('%Y-%m-%d') for i in range(30) ],
          file_path_pattern, id_columns)
    """

    def get_extender_month(start_date):
        df_list = []
          
        for i in range(1, 30):
            d = ( start_date + timedelta(i) ).strftime('%Y-%m-%d')
            # at d day, whether home connected with router exclusive or extender
            try:
                df = spark.read.json(hdfs_pd + f"user/maggie/temp1.1/wifiScore_detail{d}.json")\
                            .withColumn( "Rou_Ext", F.explode( "Rou_Ext" ))\
                            .groupBy("date","serial_num").agg( max("Rou_Ext").alias("Rou_Ext") )
                df_list.append(df)
            except Exception as e:
                print(e)
                print(d)
        
        union_df = union_df_list(df_list)
        union_df.repartition(100)\
                .write.parquet(hdfs_pd + f"/user/ZheS/wifi_score_v2/Rou_ext_{start_date.strftime('%b')}")

    get_extender_month( datetime(2023, 12 , 1)  )





    
    # comparing the Rou_Ext column between two adjacent day,
    # if the difference is 1, then add new extender;
    # if the difference is 0, then keep using exclusively router
    # but, there are many -1, this methodology can not distinct extender family
    def get_extender_daily(date_val): 
        """ 
        Process two JSON files for the given date and return a DataFrame with differences. 
        """ 

        file_path1 = f"/user/maggie/temp1.1/wifiScore_detail{ date_val.strftime('%Y-%m-%d') }.json" 
        file_path2 = f"/user/maggie/temp1.1/wifiScore_detail{ (date_val- timedelta(1) ).strftime('%Y-%m-%d')  }.json" 
        df1 = spark.read.json(file_path1) 
        df2 = spark.read.json(file_path2) 
        
        df_device1 = ( 
            df1.withColumn("Rou1", F.explode(col("Rou_Ext"))) 
            .groupBy("serial_num").max("Rou1") 
            .withColumnRenamed("max(Rou1)", "Rou_today") 
        ) 
        
        df_device2 = ( 
            df2.withColumn("Rou2", F.explode(col("Rou_Ext"))) 
            .groupBy("serial_num").max("Rou2") 
            .withColumnRenamed("max(Rou2)", "Rou_yest") 
        ) 
        
        df_join = df_device1.join(df_device2, "serial_num").withColumn("diff", col("Rou_today") - col("Rou_yest")) 
        
        return df_join


