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

def filter_serial_num(date, condition): 
    hdfs_path = hdfs_pd + "/user/ZheS/wifi_score_v3/homeScore_dataframe/" + date 
    df = spark.read.parquet(hdfs_path)\
        .groupby("serial_num") \
        .count() \
        .filter(condition) \
        .select("serial_num") 
    return df 
def before_extender(date): 
    return filter_serial_num(date, col("count") == 1) 
def after_extender(date): 
    return filter_serial_num( date, col("count") == 2) 

def agg_device( date_range, df_extender, file_path_pattern = None, columns_to_agg = None,  feature_columns = None):

    device_path = hdfs_pd + "/user/ZheS/wifi_score_v3/deviceScore_dataframe/{}"
    file_path_pattern = device_path
    
    columns_to_agg = ["avg_phyrate", "poor_phyrate", "poor_rssi", "device_score", "weights",'stationarity',"volume",
                                          'avg_sig_strength_cat1', 'avg_sig_strength_cat2', 'avg_sig_strength_cat3']
                                              
    feature_columns = columns_to_agg + ['Rou_Ext', 'cust_id', 'date', 'dg_model', 'firmware', 'mdn', 'rk_row_sn', 'rowkey',  'serial_num', 'station_mac']
    
    
    df_list = [] 
    for d in date_range: 
        file_path = file_path_pattern.format( d )
        try:
            df_kpis = spark.read.parquet(file_path).select(feature_columns)
            # after reading the deviceScore dataframe, can further join deviceFeature
            df_list.append(df_kpis)
        except Exception as e:
            print("agg_device",e,file_path)  
    Extenders = reduce(lambda df1, df2: df1.unionAll(df2), df_list).join( F.broadcast(df_extender), "serial_num" )
    
    
    average_columns = [F.avg(col).alias(col) for col in columns_to_agg]
    median_columns = [ F.expr(f"percentile_approx({col}, {0.5})").alias(f"median_{col}") 
                        for col in columns_to_agg if col != "device_score"] 

    result_df = Extenders.groupBy("serial_num")\
                        .agg(*average_columns, 
                                *median_columns, 
                                collect_list("device_score").alias("device_scores_list"),
                                F.count("*").alias("count")) 
    #------------------------------------------------------------------------
    def get_lowest_n_udf(low_n): 
        @udf(FloatType())
        def get_lowest_n(scores_list): 
            numbers_list = sorted(scores_list)[:low_n]
            return float( np.sum(numbers_list) / len(numbers_list) )
        return get_lowest_n
    low_n = 5
    current_udf = get_lowest_n_udf(low_n) 
    result_df = result_df.withColumn(f"lowest_n_scores", current_udf("device_scores_list"))\
                            .drop("device_scores_list")
    #------------------------------------------------------------------------
    numeric_columns = [e for e in result_df.columns if e != "serial_num"]
    return round_numeric_columns(result_df, decimal_places= 4, numeric_columns = numeric_columns )

def agg_home( date_range, df_extender, modelLabel = True, file_path_pattern = None,   id_columns = None ):   
    home_path = hdfs_pd + "/user/ZheS/wifi_score_v3/homeScore_dataframe/{}"
    file_path_pattern = home_path 
    id_columns = ["serial_num","home_score","dg_model_indiv","num_station"] 
    routers = ["G3100","CR1000A","XCI55AX","ASK-NCQ1338FA","CR1000B","WNC-CR200A"]
    other_routers = ["FWA55V5L","FWF100V5L","ASK-NCM1100E","ASK-NCM1100","ASK-NCQ1338","FSNO21VA","ASK-NCQ1338E"]
    extender = ["E3200","CE1000A","CME1000"]
    df_list = [] 
    for d in date_range: 
        file_path = file_path_pattern.format( d )
        try:
            df_kpis = spark.read.parquet(file_path).select(id_columns).join( df_extender, "serial_num" )
            df_list.append(df_kpis)

        except Exception as e:
            print("agg_home",e,file_path)   
    
    home_ext = reduce(lambda df1, df2: df1.unionAll(df2), df_list)
    if modelLabel:
        result_df = home_ext.filter(col("dg_model_indiv").isin(routers+other_routers))\
                            .groupBy("serial_num","dg_model_indiv")\
                            .agg( 
                                 avg("home_score").alias("home_score"),
                                 avg("num_station").alias("num_station")
                                 )
        return round_numeric_columns(result_df, decimal_places= 4, numeric_columns = ["home_score","num_station"] )

    else:
        result_df = home_ext.filter(col("dg_model_indiv").isin(routers+other_routers))\
                            .groupBy("serial_num")\
                            .agg( 
                                 avg("home_score").alias("home_score")
                                 )
        return round_numeric_columns(result_df, decimal_places= 4, numeric_columns = ["home_score"] )


    
if __name__ == "__main__":
    
    spark = SparkSession.builder\
            .appName('testScript_ZheS')\
            .config("spark.sql.adapative.enabled","true")\
            .getOrCreate()
    #
    hdfs_pd = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/"
    start_date = date.today() - timedelta(8)
    try:
        
        # inner join [previous only one router family] with [after with more than just one router/extender]
        before_dates = [ (start_date - timedelta(i+1) ).strftime('%Y-%m-%d') for i in range(7)]  #exclude start date
        before_dfs = map(lambda date: before_extender(date), before_dates) 
        df_before = reduce(lambda df1, df2: df1.join(df2, "serial_num"), before_dfs) 

        after_dates = [ ( start_date + timedelta(i) ).strftime('%Y-%m-%d') for i in range(8)]  # include start date
        after_dfs = map(lambda date: after_extender(date), after_dates) 
        df_after = reduce(lambda df1, df2: df1.join(df2, "serial_num"), after_dfs) 

        df_extenderList = df_after.join( df_before, "serial_num" )

        df_before_feature = agg_device(before_dates, df_extenderList )
        df_before_home_score = agg_home(before_dates, df_extenderList ).withColumnRenamed("home_score", "before_home_score")

        df_after_home_score = agg_home(after_dates[1:], df_extenderList, False ).withColumnRenamed("home_score", "after_home_score")

        df_before_feature.join(df_before_home_score,"serial_num")\
                        .join(df_after_home_score,"serial_num")\
                        .withColumn("date_string", lit(start_date.strftime('%Y-%m-%d')))\
                        .write.mode("overwrite")\
                        .parquet( hdfs_pd + "/user/ZheS/wifi_score_v3/installExtenders/" + (start_date).strftime("%Y-%m-%d") )

    except Exception as e:
        print(start_date, e)
    sys.exit()
    start_date = date(2024, 3, 22) 
    while start_date < date.today() - timedelta(7):

        try:
            
            # inner join [previous only one router family] with [after with more than just one router/extender]
            before_dates = [ (start_date - timedelta(i+1) ).strftime('%Y-%m-%d') for i in range(7)]  #exclude start date
            before_dfs = map(lambda date: before_extender(date), before_dates) 
            df_before = reduce(lambda df1, df2: df1.join(df2, "serial_num"), before_dfs) 

            after_dates = [ ( start_date + timedelta(i) ).strftime('%Y-%m-%d') for i in range(8)]  # include start date
            after_dfs = map(lambda date: after_extender(date), after_dates) 
            df_after = reduce(lambda df1, df2: df1.join(df2, "serial_num"), after_dfs) 

            df_extenderList = df_after.join( df_before, "serial_num" )

            df_before_feature = agg_device(before_dates, df_extenderList )
            df_before_home_score = agg_home(before_dates, df_extenderList ).withColumnRenamed("home_score", "before_home_score")

            df_after_home_score = agg_home(after_dates[1:], df_extenderList, False ).withColumnRenamed("home_score", "after_home_score")

            df_before_feature.join(df_before_home_score,"serial_num")\
                            .join(df_after_home_score,"serial_num")\
                            .withColumn("date_string", lit(start_date.strftime('%Y-%m-%d')))\
                            .write.mode("overwrite")\
                            .parquet( hdfs_pd + "/user/ZheS/wifi_score_v3/installExtenders/" + (start_date).strftime("%Y-%m-%d") )

        except Exception as e:
            print(start_date, e)
        
        start_date = start_date + timedelta(1)