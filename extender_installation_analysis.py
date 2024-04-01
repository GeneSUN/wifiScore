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
            columns_to_agg,
            device_path,
            home_path
        ) -> None:
        self.spark = sparksession
        self.install_extender_date = install_extender_date # e.g. 10
        self.window_range = window_range # e.g. 30
        self.columns_to_agg = columns_to_agg
        self.device_path = device_path
        self.home_path = home_path
        self.before_extender_date = self.install_extender_date - timedelta( self.window_range )
        self.after_extender_date = self.install_extender_date + timedelta( self.window_range )
        

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
 
    def agg_home(self,  date_range, df_extender, file_path_pattern = None, columns_to_agg = None,  id_columns = None ):   
        if file_path_pattern is None:
            file_path_pattern = self.home_path 
        if columns_to_agg is None:
            columns_to_agg = ["home_score"] 
        if id_columns is None:
            id_columns = ["serial_num","home_score"]
        
        df_list = [] 
        for d in date_range: 
            file_path = file_path_pattern.format( d )
            try:
                df_kpis = spark.read.parquet(file_path).select(id_columns).join( broadcast(df_extender), "serial_num" )
                df_list.append(df_kpis)
                print(file_path)
            except Exception as e:
                print("agg_home",e,file_path)   
        
        home_ext = reduce(lambda df1, df2: df1.unionAll(df2), df_list)
        result_df = home_ext.groupBy("serial_num").agg( avg("home_score").alias("home_score"))
        
        return round_numeric_columns(result_df, decimal_places= 4, numeric_columns = ["home_score"] )
        
    def agg_device(self, date_range, df_extender, file_path_pattern = None, columns_to_agg = None,  feature_columns = None):
        if file_path_pattern is None:
            file_path_pattern = self.device_path
        if columns_to_agg is None:
            columns_to_agg = self.columns_to_agg 
        if feature_columns is None:
            feature_columns = columns_to_agg + ['Rou_Ext', 'cust_id', 'date', 'dg_model', 'firmware', 'mdn', 'rk_row_sn', 'rowkey',  'serial_num', 'station_mac']
        df_list = [] 
        for d in date_range: 
            file_path = file_path_pattern.format( d )
            try:
                df_kpis = spark.read.parquet(file_path).select(feature_columns)
                df_list.append(df_kpis)
                #print(file_path)
            except Exception as e:
                print("agg_device",e,file_path)     
        Extenders = reduce(lambda df1, df2: df1.unionAll(df2), df_list).join( broadcast(df_extender), "serial_num" )
        
        average_columns = [F.avg(col).alias(col) for col in columns_to_agg]
        median_columns = [ F.expr(f"percentile_approx({col}, {0.5})").alias(f"median_{col}") 
                            for col in columns_to_agg if col != "device_score"] 
    
        result_df = Extenders.groupBy("serial_num")\
                            .agg(*average_columns, 
                                    *median_columns, 
                                    collect_list("device_score").alias("device_scores_list"),
                                    F.sum(exp(col("volume")) ).alias("total_volume"),
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
        df2 = self.serialNum_RouExt( start_date = install_extender_date )
        df3 = self.serialNum_RouExt( start_date = after_extender_date )
        df_extender = df1.filter( col("Rou_Ext") == "0" )\
                        .join( df2.filter( col("Rou_Ext") == "1" ), "serial_num" )\
                        .join( df3.filter( col("Rou_Ext") == "1" ), "serial_num" )\
                        .select("serial_num")
                        
        # 2. join before extender features and after extender features (exclude install extender month)
        after_range = [ ( after_extender_date + timedelta(i) ).strftime('%Y-%m-%d') for i in range(date_window) ]
        after_install_features = self.agg_device(after_range, df_extender)\
                                        .select( "serial_num", 
                                                col("device_score").alias("target_avg_device_score"),
                                                col("lowest_n_scores").alias("target_lowest_n_scores"),
                                                )
        
        before_range = [ ( before_extender_date + timedelta(i) ).strftime('%Y-%m-%d') for i in range(date_window) ]
        before_install_features = self.agg_device(before_range,  df_extender)\
                                        .withColumnRenamed("device_score","avg_device_score")\
                                        .withColumnRenamed("lowest_n_scores","before_lowest_n_scores")
                                        

        before_install_homescore = self.agg_home(before_range,  df_extender).withColumnRenamed("home_score","before_home_score")
        after_install_homescore = self.agg_home(after_range,  df_extender).withColumnRenamed("home_score","after_home_score")

        df_ext_bef_aft = before_install_features.join(before_install_homescore, "serial_num" )\
                                                .join(after_install_features, "serial_num" )\
                                                .join(after_install_homescore, "serial_num" )
        
        return df_ext_bef_aft
    def add_modelName(self):
        # it does not matter what date of dg_model_indiv, because device is constant
        start_d = self.install_extender_date 
        routers = ["G3100","CR1000A","XCI55AX","ASK-NCQ1338FA","CR1000B","CR1000B","WNC-CR200A","ASK-NCQ1338","FSNO21VA","ASK-NCQ1338E"]
        other_routers = ["FWA55V5L","FWF100V5L","ASK-NCM1100E","ASK-NCM1100"]
        df_deviceModel = spark.read.parquet( hdfs_pd + f"/user/ZheS/wifi_score_v2/homeScore_dataframe/{start_d}" )\
                        .filter(col("dg_model_indiv").isin(routers + other_routers))\
                        .withColumn("dg_model_indiv", 
                                       when(col("dg_model_indiv").isin(other_routers), "others")
                                       .otherwise(col("dg_model_indiv")))\
                        .select("serial_num",
                                "dg_model_indiv",
                                "home_score"
                                )

        
    
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
    
    columns_to_agg = ["avg_phyrate", "poor_phyrate", "poor_rssi", "device_score", "weights",'stationarity',"volume",
                                              'avg_sig_strength_cat1', 'avg_sig_strength_cat2', 'avg_sig_strength_cat3']
    columns_to_agg = ["avg_phyrate", "poor_phyrate", "poor_rssi", "device_score", "weights",'stationarity',"volume"]
    
    inst_v3 = extenders_parquet( sparksession = spark, 
                            install_extender_date = d, 
                            window_range = window_range,
                            columns_to_agg = columns_to_agg, 
                            device_path = hdfs_pd + "/user/ZheS/wifi_score_v3/deviceScore_dataframe/{}",
                            home_path = hdfs_pd + "/user/ZheS/wifi_score_v3/homeScore_dataframe/{}"
                            )

    output_path = (
                hdfs_pd + "/user/ZheS/wifi_score_v3/training_dataset/" + \
                f"{inst_v3.before_extender_date.strftime('%Y-%m-%d')  }_" +\
                f"{inst_v3.install_extender_date.strftime('%Y-%m-%d')  }_" +\
                f"{inst_v3.after_extender_date.strftime('%Y-%m-%d')  }_" +\
                f"window_range_{inst_v3.window_range}"
                )
    inst_v3.df_ext_bef_aft.write.mode("overwrite").parquet(output_path)


    """

    inst_v2 = extenders_parquet( sparksession = spark, 
                        install_extender_date = d, 
                        window_range = window_range,
                        columns_to_agg = ["avg_phyrate", "poor_phyrate", "poor_rssi", "device_score", "weights",'stationarity'], 
                        device_path = hdfs_pd + "/user/ZheS/wifi_score_v2/deviceScore_dataframe/{}",
                        home_path = hdfs_pd + "/user/ZheS/wifi_score_v2/homeScore_dataframe/{}"
                        )
    output_path = (
                    hdfs_pd + "/user/ZheS/wifi_score_v2/training_dataset/" + \
                    f"{inst_v2.before_extender_date.strftime('%Y-%m-%d')  }_" +\
                    f"{inst_v2.install_extender_date.strftime('%Y-%m-%d')  }_" +\
                    f"{inst_v2.after_extender_date.strftime('%Y-%m-%d')  }_" +\
                    f"window_range_{inst_v2.window_range}"
                    )
    inst_v2.df_ext_bef_aft.write.mode("overwrite").parquet(output_path)


    """

