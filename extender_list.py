from pyspark.sql import functions as F 
from pyspark.sql.functions import (collect_list,concat,from_unixtime,lpad, broadcast, sum, udf, col, abs, length, min, max, lit, avg, when, concat_ws, to_date, exp, explode,countDistinct, first,round  ) 
from pyspark.sql import SparkSession 
from pyspark.sql.types import FloatType
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta 
import numpy as np
from dateutil.parser import parse
import argparse 
import subprocess
from functools import reduce 
import sys
sys.path.append('/usr/apps/vmas/script/ZS/SNAP') 
from MailSender import MailSender

def check_hdfs_files(path, filename):
    # check if file exist
    ls_proc = subprocess.Popen(['/usr/apps/vmas/hadoop-3.3.6/bin/hdfs', 'dfs', '-du', path], stdout=subprocess.PIPE)

    ls_proc.wait()
    # check return code
    ls_lines = ls_proc.stdout.readlines()
    all_files = []
    for i in range(len(ls_lines)):
        all_files.append(ls_lines[i].split()[-1].decode('utf-8').split('/')[-1])

    if filename in all_files:
        return True
    return False
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
            .appName('extender_list_ZheS')\
            .config("spark.sql.adapative.enabled","true")\
            .getOrCreate()
    #
    hdfs_pd = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/"

    parser = argparse.ArgumentParser(description="Inputs") 
    parser.add_argument("--date", default=(date.today()).strftime("%Y-%m-%d")) 
    args = parser.parse_args()
    date_today:str= args.date
    d_range = [(datetime.strptime(date_today, "%Y-%m-%d") - timedelta(days=day)) for day in range(10)]  
    
    for date_val in d_range:
        start_date = date_val - timedelta(8)
        date_str = (start_date).strftime("%Y-%m-%d")
        if check_hdfs_files(hdfs_pd + "/user/ZheS/wifi_score_v3/installExtenders/", date_str):
            print(f'6x6 data for {date_str} already exists')
            continue
        else:
            print('Start to process data for ' + date_str)
        
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