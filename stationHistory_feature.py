from datetime import datetime, timedelta, date
from pyspark.sql.window import Window
from pyspark.sql.functions import concat, sum, concat_ws, col, split, concat_ws, lit ,udf,count, max,lit,avg, when,concat_ws,to_date,explode
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys 

import argparse
from html import parser
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import max,lit
from pyspark.sql.functions import udf,when,concat_ws
from pyspark.sql.functions import col,to_date
from pyspark.sql.functions import explode 
from datetime import datetime, timedelta, date
from pyspark.sql.window import Window
from pyspark.sql.functions import sum, concat_ws, col, split, concat_ws, lit ,udf,count, max,lit,avg, when,concat_ws,to_date,explode
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def flatten_df_v2(nested_df):
    # flat the nested columns and return as a new column
    flat_cols = [c[0] for c in nested_df.dtypes if c[1][:6] != 'struct']
    nested_cols = [c[0] for c in nested_df.dtypes if c[1][:6] == 'struct']
    array_cols = [c[0] for c in nested_df.dtypes if c[1][:5] == "array"]
    #print(len(nested_cols))
    if len(nested_cols)==0 and len(array_cols)==0 :
        #print(" dataframe flattening complete !!")
        return nested_df
    elif len(nested_cols)!=0:
        flat_df = nested_df.select(flat_cols +
                                   [F.col(nc+'.'+c).alias(nc+'_b_'+c)
                                    for nc in nested_cols
                                    for c in nested_df.select(nc+'.*').columns])
        return flatten_df_v2(flat_df)
    elif len(array_cols)!=0:
        for array_col in array_cols:
            flat_df = nested_df.withColumn(array_col, F.explode(F.col(array_col)))
        return flatten_df_v2(flat_df) 

if __name__ == "__main__":

    spark = SparkSession.builder\
            .appName('stationHistory_feature_zhes')\
            .config("spark.sql.adapative.enabled","true")\
            .getOrCreate()
        #
    # file ------------------------------------
    datetoday = date.today()
    date_str1 = datetoday.strftime("%Y%m%d") # e.g. 20231223
    hdfs_pd = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/"
    station_history_path = hdfs_pd + "/usr/apps/vmas/sha_data/bhrx_hourly_data/StationHistory/"
    
    df_sh = (
            spark.read.parquet( station_history_path + date_str1 )\
                    .transform(flatten_df_v2)\
                    .withColumn('time', F.from_unixtime(col('ts') / 1000.0).cast('timestamp'))\
                    .withColumn('rowkey_length', F.length('rowkey'))\
                    .withColumn('serial_num', F.col('rowkey').substr(F.lit(6), F.col('rowkey_length') - F.lit(18)))\
                    .withColumn('rk_row', F.col('rowkey').substr(F.lit(1),F.lit(4)))\
                    .withColumn("rk_row_sn",concat_ws("-","rk_row","serial_num"))
                    .withColumn('sdcd_tx_link_rate', F.regexp_replace('Station_Data_connect_data_b_tx_link_rate', "Mbps", "") )\
                    .withColumn('sdcd_tx_link_rate', F.col('sdcd_tx_link_rate').cast("long") )
                    .withColumn('sdcd_lr_length', F.length('Station_Data_connect_data_b_link_rate'))\
                    .withColumn('sdcd_link_rate', F.col('Station_Data_connect_data_b_link_rate').substr(F.lit(1), F.col('sdcd_lr_length') - F.lit(4)))\
                    .withColumn("sdcd_link_rate",col("sdcd_link_rate").cast("long"))
                    .withColumn('sdcd_connect_type', F.col('Station_Data_connect_data_b_connect_type').substr(F.lit(1), F.lit(2)))\
                    .filter((col("sdcd_connect_type").isin(['2.','5G','6G'])))
            )
    
    column_transformations ={
                                "Station_Data_connect_data_b_station_mac": ("string", "station_mac"),
                                "Station_Data_connect_data_b_parent_id": ("string", "parent_id"),
                                "Station_Data_connect_data_b_airtime_util": ("long", "airtime"), 
                                "Station_Data_connect_data_b_bw": ("long", "bandwidth"), 
                                "Station_Data_connect_data_b_channel": ("long", "channel_freq"), 
                                "Station_Data_connect_data_b_tx_packets": ("long", "trans_packets"), 
                                "Station_Data_connect_data_b_rx_packets": ("long", "recv_packets"), 
                                "Station_Data_connect_data_b_snr": ("long", "snr"), 
                                "Station_Data_connect_data_b_signal_strength": ("long", "sdcd_signal_strength"), 
                                "Station_Data_connect_data_b_diff_bs": ("long", "byte_send"), 
                                "Station_Data_connect_data_b_diff_br": ("long", "byte_received"),
                                "Station_Data_connect_data_b_connect_state": ("long", "connect_state"),
                                "Station_Data_connect_data_b_diff_tx_packets": ("long", "diff_tx_packets"),
                                "Station_Data_connect_data_b_diff_rx_packets": ("long", "diff_rx_packets"),
                            }
    
    for original_col, (cast_type, alias_name) in column_transformations.items():
        df_sh = df_sh.withColumn(alias_name, F.col(original_col).cast(cast_type).alias(alias_name)) 

    df_selected = df_sh.select( 
                            "rowkey", 
                            "serial_num",
                            "time",
                            "sdcd_tx_link_rate", 
                            "sdcd_link_rate", 
                            "sdcd_connect_type", 
                            *[alias_name for _, (_, alias_name) in column_transformations.items()]  # Unpack the alias names for selection 
                            )

    device_ids = ["rowkey","serial_num","station_mac"]
    columns_to_aggregate = [ 
                            "sdcd_tx_link_rate", "sdcd_link_rate", "airtime", "bandwidth", 
                            "channel_freq", "trans_packets", "recv_packets", "snr", 
                            "sdcd_signal_strength", "byte_send", "byte_received",  
                            "diff_tx_packets", "diff_rx_packets" 
                            ]
    
    agg_exprs = [] 

    for column in columns_to_aggregate: 
        agg_exprs.append(F.round(F.avg(column), 2).alias(f"{column}_avg")) 
        agg_exprs.append(F.round(F.stddev(column), 2).alias(f"{column}_stddev")) 

    df_grouped = df_selected.groupBy(device_ids).agg(*agg_exprs) 

    df_grouped.dropDuplicates()\
                .withColumn("date", F.lit((datetoday - timedelta(1)).strftime('%Y-%m-%d')))\
                .repartition(100)\
                .write.mode("overwrite")\
                .parquet( hdfs_pd + "/user/ZheS/wifi_score_v3/deviceFeatures/" + (datetoday - timedelta(1)).strftime("%Y-%m-%d") )