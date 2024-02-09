
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
    
def read_json_file_by_date(date, file_path_pattern): 

    file_path = file_path_pattern.format(date)
    try: 
        df = spark.read.json(file_path)\
                    .withColumn( "Rou_Ext", F.explode( "Rou_Ext" ))\
                    .groupBy("date","serial_num").agg( max("Rou_Ext").alias("Rou_Ext") )
        return df
    except Exception as e:
        print(e)

def process_json_files_for_date_range(date_range, file_path_pattern): 

    df_list = list(map(lambda date: read_json_file_by_date(date, file_path_pattern), date_range)) 
    df_list = list(filter(None, df_list)) 
    result_df = reduce(lambda df1, df2: df1.union(df2), df_list) 

    return result_df 
    
class router_json():
    global hdfs_pd
    hdfs_pd = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/"

    def __init__(self, 
            sparksession,
            install_extender_date,
            window_range
            
        ) -> None:
        self.spark = sparksession
        self.install_extender_date = install_extender_date # e.g. date(2023, 10, 1)
        self.window_range = window_range # e.g. 30
        
        self.device_path = hdfs_pd + "user/maggie/temp1.1/wifiScore_detail{}.json"
        self.before_extender_date = self.install_extender_date - timedelta( self.window_range )
        self.df_rou_features = self.get_router_bef()

    def get_router_bef(self, before_extender_date = None, date_window = None):
        
        if before_extender_date is None:
            before_extender_date = self.before_extender_date
        if date_window is None:
            date_window = self.window_range

        before_range = [ ( before_extender_date + timedelta(i) ).strftime('%Y-%m-%d') for i in range(date_window) ]
        before_install_features = self.agg_device(before_range)\
                                        .withColumnRenamed("score","avg_device_score")

        return before_install_features
        
    def agg_device(self, date_range, file_path_pattern = None, columns_to_agg = None,  id_columns = None):
        if file_path_pattern is None:
            file_path_pattern = self.device_path
        if columns_to_agg is None:
            columns_to_agg = ["avg_phyrate", "poor_phyrate", "poor_rssi", "score", "weights", ] 
        if id_columns is None:
            id_columns = ['avg_phyrate', 'poor_phyrate', 'poor_rssi', 'score', 'serial_num', 'weights']

        df_list = [] 
        for d in date_range: 
            file_path = file_path_pattern.format( d )
            try:
                df_kpis = spark.read.json(file_path).select(id_columns)
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
                                            F.count("*").alias("count")) 
    
        numeric_columns = [e for e in result_df.columns if e != "serial_num"]
        return round_numeric_columns(result_df, decimal_places= 4, numeric_columns = numeric_columns )



if __name__ == "__main__":
    # the only input is the date which is used to generate 'date_range'
    spark = SparkSession.builder.appName('ZheS_wifiscore_preprocess')\
                        .config("spark.sql.adapative.enabled","true")\
                        .getOrCreate()
    hdfs_pd = 'hdfs://njbbvmaspd11.nss.vzwnet.com:9000/'

    inst1 = router_json( sparksession = spark, install_extender_date = date(2023, 10, 1), window_range = 7)

    output_path = (
                hdfs_pd + "/user/ZheS/wifi_score_v2/training_dataset/" + \
                f"router_{inst1.install_extender_date.strftime('%Y-%m-%d')  }_" +\
                f"window_range_{inst1.window_range}"
                )
    

    inst1.df_rou_features.write.mode("overwrite").parquet(output_path)












