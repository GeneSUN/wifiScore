
from pyspark.sql import functions as F 
from pyspark.sql.functions import (collect_list, concat,from_unixtime,lpad, broadcast, sum, udf, col, abs, length, min, max, lit, avg, when, concat_ws, to_date, exp, explode,countDistinct, first,round  ) 
from pyspark.sql import SparkSession 
from pyspark.sql.types import FloatType
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta 
from dateutil.parser import parse
import argparse 
from functools import reduce 
import sys
import numpy as np

def round_numeric_columns(df, decimal_places=2, numeric_columns = None): 

    if numeric_columns == None:
        numeric_columns = [col_name for col_name, data_type in df.dtypes if data_type == "double" or data_type == "float"]

    # Apply rounding to all numeric columns 
    for col_name in numeric_columns: 
        df = df.withColumn(col_name, round(df[col_name], decimal_places)) 
        
    return df
    
class router_parquet():
    global hdfs_pd, columns_to_agg
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
        self.install_extender_date = install_extender_date # e.g. date(2023, 10, 1)
        self.window_range = window_range # e.g. 30
        self.columns_to_agg = columns_to_agg
        self.device_path = device_path
        self.home_path = home_path
        self.before_extender_date = self.install_extender_date - timedelta( self.window_range )
        self.df_rou_features = self.get_router_bef()

    def get_router_bef(self, before_extender_date = None, date_window = None):
        
        if before_extender_date is None:
            before_extender_date = self.before_extender_date
        if date_window is None:
            date_window = self.window_range

        before_range = [ ( before_extender_date + timedelta(i) ).strftime('%Y-%m-%d') for i in range(date_window) ]
        before_install_features = self.agg_device(before_range)\
                                        .withColumnRenamed("device_score","avg_device_score")\
                                        .withColumnRenamed("lowest_n_scores","before_lowest_n_scores")
        
        before_install_homescore = self.agg_home(before_range)\
                                        .withColumnRenamed("home_score","before_home_score")
        

        return before_install_features.join(before_install_homescore,"serial_num")
    
    def agg_home(self,  date_range, file_path_pattern = None, columns_to_agg = None,  id_columns = None ):   
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
                df_kpis = spark.read.parquet(file_path).select(id_columns)
                df_list.append(df_kpis)
                print(file_path)
            except Exception as e:
                print("agg_home",e,file_path)   
        
        home_ext = reduce(lambda df1, df2: df1.unionAll(df2), df_list)
        result_df = home_ext.groupBy("serial_num").agg( avg("home_score").alias("before_home_score"))
        
        return round_numeric_columns(result_df, decimal_places= 4, numeric_columns = ["before_home_score"] )

    def agg_device(self, date_range, file_path_pattern = None, columns_to_agg = None, feature_columns = None):
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
                print(file_path)
            except Exception as e:
                print(e)    
        routers = reduce(lambda df1, df2: df1.unionAll(df2), df_list)
        
        average_columns = [F.avg(col).alias(col) for col in columns_to_agg]
        median_columns = [ F.expr(f"percentile_approx({col}, {0.5})").alias(f"median_{col}") 
                            for col in columns_to_agg if col != "score"] 
    
        result_df = routers.groupBy("serial_num")\
                                    .agg(*average_columns, 
                                            *median_columns, 
                                            collect_list("device_score").alias("device_scores_list"),
                                            F.sum(exp(col("volume")) ).alias("total_volume"),
                                            F.count("*").alias("count")) 
    
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



if __name__ == "__main__":
    # the only input is the date which is used to generate 'date_range'
    spark = SparkSession.builder.appName('ZheS_wifiscore_preprocess')\
                        .config("spark.sql.adapative.enabled","true")\
                        .getOrCreate()
    hdfs_pd = 'hdfs://njbbvmaspd11.nss.vzwnet.com:9000/'
    columns_to_agg = ["avg_phyrate", "poor_phyrate", "poor_rssi", "device_score", "weights",'stationarity',"volume","avg_sig_strength_cat1","avg_sig_strength_cat2"]

    inst1 = router_parquet( sparksession = spark, 
                            install_extender_date = date.today() - timedelta(1), 
                            window_range = 7, 
                            columns_to_agg= columns_to_agg,
                            device_path = hdfs_pd + "/user/ZheS/wifi_score_v3/deviceScore_dataframe/{}",
                            home_path = hdfs_pd + "/user/ZheS/wifi_score_v3/homeScore_dataframe/{}"
                            )

    output_path = (
                hdfs_pd + "/user/ZheS/wifi_score_v3/router_dataset_Model_deploy/" + \
                f"router_{inst1.install_extender_date.strftime('%Y-%m-%d')  }_" +\
                f"window_range_{inst1.window_range}"
                )
    

    inst1.df_rou_features.write.mode("overwrite").parquet(output_path)












