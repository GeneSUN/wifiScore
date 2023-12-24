import sys 
sys.path.append('/usr/apps/vmas/script/ZS/SNAP') 
sys.path.append('/usr/apps/vmas/script/ZS') 
from MailSender import MailSender

from pyspark.sql.functions import sum, concat_ws, col, split, concat_ws, lit ,udf,count, max,lit,avg, when,concat_ws,to_date,explode
import argparse
from html import parser
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pyspark import SparkConf, SparkContext

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql.types import StructType, StructField, StringType
from datetime import datetime, timedelta, date
import pandas as pd
import numpy as np
import psycopg2



def outlier_rssi_poor(dataframe):
    """
    Detects and treats outliers using IQR for multiple variables in a Pandas DataFrame
    Get Stationarity of devices, stationary->1, non-stationary->0
    """
    # Calculate Q1, Q3, and IQR
    df = dataframe.sort_values("sdcd_signal_strength")
    q1, q3 = np.percentile(df["sdcd_signal_strength"],[10,90])
    iqr = q3 - q1
    # Define the upper and lower bounds for outliers
    # change factor to 2?
    lower_bound = q1 - 2 * iqr
    # Filter outliers and update the DataFrame
    dfsh_iqr = dataframe[(dataframe["sdcd_signal_strength"]>=lower_bound)]
    count_iqr = len(dataframe)-len(dfsh_iqr)

    # Filter the last 3% of data
    per5 = np.percentile(df["sdcd_signal_strength"],3)
    dfsh_per5 = dataframe[(dataframe["sdcd_signal_strength"]>=per5)]
    count_per5 = len(dataframe)-len(dfsh_per5)

    # get median and P90 of device signal strength
    med,p90 = np.percentile(df["sdcd_signal_strength"],[50,90])
    dif = np.abs(p90-med)

    # compare the number dropped and choose the less and get stationarity of device
    if count_iqr < count_per5:
        if dif <= 5:
            dfsh_iqr["rk_row"] = "1"
        else:
            dfsh_iqr["rk_row"] = "0"
        return dfsh_iqr
    else:
        if dif <= 5:
            dfsh_per5["rk_row"] = "1"
        else:
            dfsh_per5["rk_row"] = "0"
        return dfsh_per5

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

def SH_process(dfsh):
    # process the columns so that we can get what we want from Station History data
    dfsh_new = dfsh.withColumn('rowkey_length', F.length('rowkey'))\
                    .withColumn('rk_sn_row', F.col('rowkey').substr(F.lit(6), F.col('rowkey_length') - F.lit(18)))\
                    .withColumn('rk_sn', F.col('rowkey').substr(F.lit(6), F.col('rowkey_length') - F.lit(22)))\
                    .withColumn('rk_sn_length', F.length('rk_sn'))\
                    .withColumn('rk_row', F.col('rowkey').substr(F.lit(1),F.lit(4)))\
                    .withColumn("rk_row_sn",concat_ws("-","rk_row","rk_sn_row"))\
                    .withColumn('sdcd_tlr_length', F.length('Station_Data_connect_data_b_tx_link_rate'))\
                    .withColumn('sdcd_tx_link_rate', F.col('Station_Data_connect_data_b_tx_link_rate').substr(F.lit(1), F.col('sdcd_tlr_length') - F.lit(4)))\
                    .withColumn("sdcd_tx_link_rate",col("sdcd_tx_link_rate").cast("long"))\
                    .withColumn('sdcd_lr_length', F.length('Station_Data_connect_data_b_link_rate'))\
                    .withColumn('sdcd_link_rate', F.col('Station_Data_connect_data_b_link_rate').substr(F.lit(1), F.col('sdcd_lr_length') - F.lit(4)))\
                    .withColumn("sdcd_link_rate",col("sdcd_link_rate").cast("long"))\
                    .withColumn('sdcd_connect_type', F.col('Station_Data_connect_data_b_connect_type').substr(F.lit(1), F.lit(2)))\
                    .withColumn("sdcd_signal_strength", F.col("Station_Data_connect_data_b_signal_strength").cast("long"))
    return dfsh_new
    
def DG_process(dfdg):
    # get and process the columns we need from Device Group data
    dfdg_out = dfdg.select(col("rowkey").alias("dg_rowkey")
                            ,col("col_b_mac").alias("RouExt_mac")
                            ,col("col_b_model").alias("dg_model")
                            ,col("col_b_parent_mac").alias("parent_mac")
                            ,col("col_b_fw").alias("firmware"))\
                    .groupby("dg_rowkey","RouExt_mac","dg_model")\
                        .agg(max("parent_mac").alias("parent_mac")
                             ,max("firmware").alias("firmware"))
    return dfdg_out
    
def get_Home(dfsh):
    # get required columns and filter connect_type to have only wireless data
    dfsh_out = dfsh.filter((dfsh.sdcd_connect_type.isin(['2.','5G','6G'])))\
                    .select("rowkey"
                            ,"rk_sn_row"
                            ,"rk_row_sn"
                            ,"rk_row"
                            ,"Station_Data_connect_data_b_station_mac"
                            ,"Station_Data_connect_data_b_parent_id"
                            ,"sdcd_signal_strength"
                            ,"sdcd_tx_link_rate"
                            ,"sdcd_link_rate"
                            ,"sdcd_connect_type")\
                    .withColumnRenamed("Station_Data_connect_data_b_station_mac","station_mac")\
                    .withColumnRenamed("Station_Data_connect_data_b_parent_id","parent_id")\
                    .withColumnRenamed("rk_sn_row","serial_num")
    return dfsh_out

def get_rssi(dfsh):
    # get all rowkey_related, station_mac, parent id, rssi --> new table
    dfsh_out = dfsh.select("rowkey"
                            ,"serial_num"
                            ,"rk_row_sn"
                            ,"rk_row"
                            ,"station_mac"
                            ,"parent_id"
                            ,"sdcd_signal_strength"
                            ,"sdcd_connect_type").dropna(subset="sdcd_signal_strength")
    return dfsh_out

def get_phyrate(dfsh):
    # get all rowkey_related, station_mac, parent id, phy_rate --> new table
    dfsh_out = dfsh.filter((dfsh.sdcd_tx_link_rate>6)
                            & (dfsh.sdcd_tx_link_rate<2500) 
                            & (~dfsh.sdcd_tx_link_rate.isin([12,24])))\
                    .select("rowkey"
                            ,"serial_num"
                            ,"rk_row_sn"
                            ,"rk_row"
                            ,"station_mac"
                            ,"parent_id"
                            ,"sdcd_tx_link_rate"
                            ,"sdcd_connect_type").dropna(subset="sdcd_tx_link_rate")
    return dfsh_out
def create_snapshot(date_str1 = None, dg_model_str = None):
    hdfs_pd = 'hdfs://njbbvmaspd11.nss.vzwnet.com:9000'
    
    #------------------------------------#------------------------------------
    if date_str1 is None:
        date_str1 = "20231219"
    if dg_model_str is None:
        dg_model_str = "ASK-NCQ1338E"
    
    #------------------------------------#------------------------------------
    date_str2 = datetime.strptime(date_str1, "%Y%m%d").strftime("%Y-%m-%d")
    p = hdfs_pd + f"/usr/apps/vmas/sha_data/bhrx_hourly_data/StationHistory/{date_str1}"
    #p =  hdfs_pd + "/user/ZheS/Practice_Parquet/station_history_sample"
    dfsh = spark.read.parquet(p)
    dfsh_flat = flatten_df_v2(dfsh)
    dfsh_flat = SH_process(dfsh_flat)
    dfsh_out = get_Home(dfsh_flat)

    path = hdfs_pd + f"/user/ZheS/wifi_score_v2/vcg_vbg/{date_str2}"
    df_vcg = spark.read.json( path ).drop("dg_model")\
                    .filter(col("dg_model_indiv") == dg_model_str)\
                    .select("serial_num").distinct()

    broadcast_df = F.broadcast(df_vcg) 
    df_snapshot = dfsh_out.join(broadcast_df, "serial_num").repartition(20)
    
    output = hdfs_pd + f"/user/ZheS/wifi_score_v2/snapshot/{date_str2}_{dg_model_str}"
    df_snapshot.write.mode("overwrite").parquet( output )


def round_columns(df,numeric_columns = None, decimal_places=4):  
    """  
    Rounds all numeric columns in a PySpark DataFrame to the specified number of decimal places. 
    Parameters: 
        df (DataFrame): The PySpark DataFrame containing numeric columns to be rounded.  
        decimal_places (int, optional): The number of decimal places to round to (default is 2).  
    Returns:  
        DataFrame: A new PySpark DataFrame with numeric columns rounded to the specified decimal places.  
    """  
    from pyspark.sql.functions import round  
    # List of numeric column names  
    if numeric_columns is None:
        numeric_columns = [col_name for col_name, data_type in df.dtypes if data_type == "double" or data_type == "float"]  

    # Apply rounding to all numeric columns  
    for col_name in numeric_columns:  
        df = df.withColumn(col_name, round(col(col_name), decimal_places))  
    return df 

def carto_query_dataframe(query, pa=None):
    dbName = "cartodb_dev_user_bc97a77f-a8e2-47d1-8a7a-280e12c50c03_db"
    serverHost = "nj51vmaspa9"
    conn = psycopg2.connect(database=dbName, user='postgres', host=serverHost, port="5432",password = '12345',options="-c statement_timeout=60min")
    cur = conn.cursor()
    cur.execute("Set statement_timeout to '60min'")
    df = pd.read_sql_query(query,conn,params=pa)
    cur.close()
    conn.close()
    return df

if __name__ == "__main__":
    hdfs_pd = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/"
    spark = SparkSession \
        .builder \
        .appName("wifiScore_ZheS") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate() 
    
    date_str1 = date.today().strftime("%Y%m%d")
    date_str2 = datetime.strptime(date_str1, "%Y%m%d").strftime("%Y-%m-%d")

    p = hdfs_pd + f"/usr/apps/vmas/sha_data/bhrx_hourly_data/StationHistory/{date_str1}"
    df_snapshot = spark.read.parquet( p )\
                    .transform(flatten_df_v2)\
                    .transform(SH_process)\
                    .transform(get_Home)
    """
    p = hdfs_pd + f"/user/ZheS/wifi_score_v2/snapshot/{date_str}_ASK-NCQ1338E"
    df_snapshot = spark.read.parquet(p)
    serial_nums = df_snapshot.select("serial_num").distinct().limit(100).rdd.map(lambda row: row[0]).collect() 
    df_snapshot = df_snapshot.filter( col("serial_num").isin(serial_nums) )
    """

    df_sdcd = df_snapshot.drop("sdcd_tx_link_rate","sdcd_link_rate")\
                                .dropna(subset="sdcd_signal_strength")
    
    # get poor_rssi
    device_ids = ["rowkey","rk_row_sn","serial_num","station_mac","parent_id"]

    condition_cat1 = (col("sdcd_connect_type") == "2.") & (col("sdcd_signal_strength") < -78) 
    condition_cat2 = (col("sdcd_connect_type") == "5G") & (col("sdcd_signal_strength") < -75) 
    condition_cat3 = (col("sdcd_connect_type") == "6G") & (col("sdcd_signal_strength") < -70) 

    df_rssi = ( 
            df_sdcd.groupBy(device_ids) 
            .agg( 
                sum(when(condition_cat1, 1).otherwise(0)).alias("count_cat1"), 
                sum(when(condition_cat2, 1).otherwise(0)).alias("count_cat2"), 
                sum( when(condition_cat3, 1).otherwise(0) ).alias("count_cat3"), 
                count("*").alias("total_count"),
            ) 
            .withColumn("poor_rssi", (col("count_cat1") + col("count_cat2") + col("count_cat3"))/col("total_count") *100 )
            .filter(col("total_count")>=36)
        ) 

    # get poor_rssi
    condition = col("sdcd_tx_link_rate") < 65
    window_spec = Window().rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing) 
    df_phy = df_snapshot.transform(get_phyrate)\
                        .groupby( device_ids )\
                        .agg( avg("sdcd_tx_link_rate").alias("avg_phyrate"), 
                            sum(when(condition, 1).otherwise(0)).alias("poor_count"), 
                            count("*").alias("total_count")
                            )\
                        .withColumn("poor_phyrate", col("poor_count")/col("total_count")*100 )\
                        .withColumn( "pre_norm_weights" ,when( col('avg_phyrate')>= 20.718, 1)
                                        .otherwise(col('avg_phyrate')/20.718) )\
                        .withColumn( 
                            "weights", 
                            F.col("pre_norm_weights")*100 / F.sum("pre_norm_weights").over(window_spec) 
                        )\
                        .drop("pre_norm_weights")
    df_phy = round_columns(df_phy, 
                                    numeric_columns = ["avg_phyrate","poor_phyrate","weights"], 
                                    decimal_places = 4
                                )
    
    p = hdfs_pd +"/usr/apps/vmas/5g_data/fixed_5g_router_mac_sn_mapping/{}/fixed_5g_router_mac_sn_mapping.csv"
    d = ( datetime.strptime(date_str2,"%Y-%m-%d") - timedelta(1) ).strftime("%Y-%m-%d")
    df_join = spark.read.option("header","true").csv(p.format(d))\
                .select( col("mdn_5g").alias("mdn"),
                        col("serialnumber").alias("serial_num"),
                        "cust_id"
                        )

    df_phy_rssi = df_phy.drop('poor_count', 'total_count')\
                    .join(df_rssi.drop('count_cat1', 'count_cat2', 'count_cat3', 'total_count'), device_ids)

    df_all = df_join.join( df_phy_rssi, "serial_num" )

    device_groups_path = hdfs_pd + "/usr/apps/vmas/sha_data/bhrx_hourly_data/DeviceGroups/" + date_str1
    dfdg = spark.read.parquet(device_groups_path)\
                .select("rowkey",explode("Group_Data_sys_info"))\
                .transform(flatten_df_v2)\
                .transform(DG_process)
    
    cond = [dfdg.dg_rowkey==df_all.rk_row_sn, dfdg.RouExt_mac==df_all.parent_id]
    df_mac =  dfdg.join(df_all,cond,"right")\
                    .withColumn("Rou_Ext",when( col("parent_mac").isNotNull(),1 ).otherwise(0) )
    
    df_deviceScore = df_mac.withColumn( 
                                    "device_score", 
                                    (100 - col("poor_rssi")) * 0.4 + (100 - col("poor_phyrate")) * 0.6 
                                )\
                            .drop("dg_rowkey")\
                            .repartition(1000)
    
    
    df_homeScore = (
                df_deviceScore.groupby("serial_num","mdn","cust_id","Rou_Ext")
                                .agg(
                                    sum(col("poor_rssi")*col("weights")).alias("poor_rssi"),\
                                    sum(col("poor_phyrate")*col("weights")).alias("poor_phyrate"),\
                                    sum(col("device_score")*col("weights")).alias("home_score"),\
                                    max("firmware").alias("firmware")
                                )
                                .repartition(1000)
                )
    

    df_deviceScore.filter( col("Rou_Ext")==1 )\
            .write.mode("overwrite")\
            .parquet( hdfs_pd + "/user/ZheS/wifi_score_v2/deviceScore_dataframe/" + date_str2 )
    df_homeScore.filter( col("Rou_Ext")==1 )\
            .write.mode("overwrite")\
            .parquet( hdfs_pd + "/user/ZheS/wifi_score_v2/homeScore_dataframe/" + date_str2 )