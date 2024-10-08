from datetime import datetime, timedelta, date
from pyspark.sql.window import Window
from pyspark.sql.functions import sum, concat_ws, col, split, concat_ws, lit ,udf,count, max,lit,avg, when,concat_ws,to_date,explode
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys 
sys.path.append('/usr/apps/vmas/script/ZS') 
from MailSender import MailSender

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

class wifiScore():
    global hdfs_pd, station_history_path, device_groups_path,serial_mdn_custid, device_ids
    hdfs_pd = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/"
    station_history_path = hdfs_pd + "/usr/apps/vmas/sha_data/bhrx_hourly_data/StationHistory/"
    device_groups_path = hdfs_pd + "/usr/apps/vmas/sha_data/bhrx_hourly_data/DeviceGroups/"
    serial_mdn_custid = hdfs_pd + "/usr/apps/vmas/5g_data/fixed_5g_router_mac_sn_mapping/{}/fixed_5g_router_mac_sn_mapping.csv"
    device_ids = ["rowkey","rk_row_sn","serial_num","station_mac"]
    
    def __init__(self, 
                sparksession,
                day
            ) -> None:
        self.spark = sparksession
        self.date_str1 = day.strftime("%Y%m%d") # e.g. 20231223
        self.date_str2 = day.strftime("%Y-%m-%d") # e.g. 2023-12-23

        self.df_stationHist = self.spark.read.parquet( station_history_path + self.date_str1 )\
                            .transform(flatten_df_v2)\
                            .transform(SH_process)\
                            .transform(get_Home)

        self.stationarity = self.filter_outlier()
        self.parent_id = self.df_stationHist.select( device_ids + ["parent_id"]).distinct()

        self.df_rssi = self.get_rssi_dataframe()
        self.df_phy = self.get_phyrate_dataframe()
        self.df_phy_weights = self.apply_weights()
        self.df_phy_rssi = self.get_rssi_phyrate_weights()
        self.df_all_features = self.add_info()
        self.df_deviceScore = self.df_all_features.withColumn( 
                                            "device_score", 
                                            (100 - col("poor_rssi")) * 0.4 + (100 - col("poor_phyrate")) * 0.6 
                                        )\
                                .drop("dg_rowkey")
        self.df_homeScore = self.df_deviceScore.groupBy("serial_num", "mdn", "cust_id")\
                    .agg(
                        count("*").alias("num_station"),    
                        F.round(F.sum(col("poor_rssi") * col("weights")), 4).alias("poor_rssi"),  
                        F.round(F.sum(col("poor_phyrate") * col("weights")), 4).alias("poor_phyrate"),  
                        F.round(F.sum(col("device_score") * col("weights")), 4).alias("home_score"), 
                        F.collect_set("dg_model").alias("dg_model"), 
                        F.collect_set("Rou_Ext").alias("Rou_Ext"), 
                    )\
                    .withColumn("date", F.lit( ( datetime.strptime(self.date_str2,"%Y-%m-%d") - timedelta(1) ).strftime("%Y-%m-%d") ))\
                    .select( "*",F.explode("dg_model").alias("dg_model_mid") ).dropDuplicates()\
                    .select( "*",F.explode("dg_model_mid").alias("dg_model_indiv") )\
                    .drop("dg_model_mid")\
                    .dropDuplicates()
        
    def filter_outlier(self, df = None, partition_columns = None, percentiles = None, column_name = None):
        if df is None:
            df = self.df_stationHist 
        if partition_columns is None:
            partition_columns = ["rowkey","rk_row_sn","station_mac","serial_num"]
        if percentiles is None:
            percentiles = [0.03, 0.1, 0.5, 0.9]
        if column_name is None:
            column_name = "sdcd_signal_strength"

        window_spec = Window().partitionBy(partition_columns) 
        
        three_percentile = F.expr(f'percentile_approx({column_name}, {percentiles[0]})') 
        ten_percentile = F.expr(f'percentile_approx({column_name}, {percentiles[1]})') 
        med_percentile = F.expr(f'percentile_approx({column_name}, {percentiles[2]})') 
        ninety_percentile = F.expr(f'percentile_approx({column_name}, {percentiles[3]})') 

        df_outlier = df.withColumn('3%_val', three_percentile.over(window_spec))\
                .withColumn('10%_val', ten_percentile.over(window_spec))\
                .withColumn('50%_val', med_percentile.over(window_spec))\
                .withColumn('90%_val', ninety_percentile.over(window_spec))\
                .withColumn("lower_bound", col('10%_val')-2*(  col('90%_val') - col('10%_val') ) )\
                .withColumn("outlier", when( col("lower_bound") < col("3%_val"), col("lower_bound")).otherwise( col("3%_val") ))\
                .filter(col(column_name) > col("outlier"))

        df_stationary = df_outlier.withColumn("diff", col('90%_val') - col('50%_val') )\
                        .withColumn("stationarity", when( col("diff")<= 5, lit("1")).otherwise( lit("0") ))\
                        .groupby(["rowkey","rk_row_sn","serial_num","station_mac"]).agg(max("stationarity").alias("stationarity"))

        return df_stationary

    def get_rssi_dataframe(self, df_stationHist = None):
        
        if df_stationHist is None:
            df_stationHist = self.df_stationHist
            
        df_sdcd = df_stationHist.drop("sdcd_tx_link_rate","sdcd_link_rate")\
                            .dropna(subset="sdcd_signal_strength")
    
        condition_cat1 = (col("sdcd_connect_type") == "2.") & (col("sdcd_signal_strength") < -78) 
        condition_cat2 = (col("sdcd_connect_type") == "5G") & (col("sdcd_signal_strength") < -75) 
        condition_cat3 = (col("sdcd_connect_type") == "6G") & (col("sdcd_signal_strength") < -70) 
    
        df_rssi = ( 
                df_sdcd.groupBy(device_ids ) 
                .agg( 
                    sum(when(condition_cat1, 1).otherwise(0)).alias("count_cat1"), 
                    sum(when(condition_cat2, 1).otherwise(0)).alias("count_cat2"), 
                    sum( when(condition_cat3, 1).otherwise(0) ).alias("count_cat3"), 
                    count("*").alias("total_count"),
                ) 
                .withColumn("poor_rssi", (col("count_cat1") + col("count_cat2") + col("count_cat3"))/col("total_count") *100 )
                .filter(col("total_count")>=36)
            ) 
        return df_rssi
        
    def get_phyrate_dataframe(self, df_stationHist = None):
        if df_stationHist is None:
            df_stationHist = self.df_stationHist
        
        # get phyrate
        condition = col("sdcd_tx_link_rate") < 65
        
        df_phy = df_stationHist.transform(get_phyrate)\
                            .groupby( device_ids )\
                            .agg( 
                                avg("sdcd_tx_link_rate").alias("avg_phyrate"), 
                                sum(when(condition, 1).otherwise(0)).alias("poor_count"), 
                                count("*").alias("total_count")
                                )\
                            .withColumn("poor_phyrate", col("poor_count")/col("total_count")*100 )
        return df_phy
        
    def apply_weights(self, df = None):
        if df is None:
            df = self.df_phy.join( self.stationarity, ["rowkey","rk_row_sn","serial_num","station_mac"], "inner")
        # Devices with low Phy Rate and classified as stationary, label avg_phyrate/20.718; others label 1
        df = df.withColumn("label", F.when( (col("avg_phyrate") <= 20.718)&(col("stationarity") == 1), 
                                                col("avg_phyrate")/20.718)\
                                            .otherwise(1)) 
        # normalized weight
        window_spec = Window.partitionBy("serial_num") 
        df = df.withColumn("sum_label", F.sum("label").over(window_spec)) 
        df = df.withColumn("weights", F.col("label") / F.col("sum_label"))\
                .drop("label","sum_label")

        df = round_columns(df, 
                                numeric_columns = ["avg_phyrate","poor_phyrate","weights"], 
                                decimal_places = 4
                            )
        return df
    def get_rssi_phyrate_weights(self,df_phy_weights = None, df_rssi = None):
        # it is suppose to define weights after joi phyrate and rssi, because some record filtered out in df_rssi
        # but i am lazy to fix that, so i re-normalize the weights here. 
        if df_phy_weights is None:
            df_phy_weights = self.df_phy_weights
        if df_rssi is None:
            df_rssi = self.df_rssi   
        
        df = df_phy_weights.drop('poor_count', 'total_count')\
                            .join( df_rssi.drop('count_cat1', 'count_cat2', 'count_cat3', 'total_count'), 
                                    device_ids)
        
        window_spec = Window().partitionBy("serial_num") 
        df_normalized = df.withColumn("total_weight", F.sum("weights").over(window_spec)) 
        df_normalized = df_normalized.withColumn("weights", F.col("weights") / F.col("total_weight"))\
                                    .drop("total_weight") 
        
        df = round_columns(df_normalized, 
                        numeric_columns = ["poor_rssi","weights"], 
                        decimal_places = 4
                    )
        return df
    def add_info(self, df_phy_rssi = None, date_str1 = None, date_str2 = None):
        if df_phy_rssi is None:
            df_phy_rssi = self.df_phy_rssi.join( self.parent_id, device_ids )
        if date_str1 is None:
            date_str1 = self.date_str1
        if date_str2 is None:
            date_str2 = self.date_str2
        
        # add mdn and cust_id
        days_before = [1, 2, 3, 4, 5] 
        for days_ago in days_before: 
            try:
                d = ( datetime.strptime(date_str2,"%Y-%m-%d") - timedelta(days_ago) ).strftime("%Y-%m-%d")
                #p = hdfs_pd +"/usr/apps/vmas/5g_data/fixed_5g_router_mac_sn_mapping/{}/fixed_5g_router_mac_sn_mapping.csv".format(d)
                p = serial_mdn_custid.format(d)
                df_join = self.spark.read.option("header","true").csv(p)\
                            .filter(col('status')=="INSTALL COMPLETE" )\
                            .select( col("mdn_5g").alias("mdn"),
                                    col("serialnumber").alias("serial_num"),
                                    "cust_id"
                                    )
                break
            except:
                pass
        df_all = df_join.join( df_phy_rssi, "serial_num", "right" )
        
        # add device group information and Router/Extender
        days_before = [0, 1, 2, 3, 4, 5] 
        for days_ago in days_before: 
            try:
                d = ( datetime.strptime(date_str1,"%Y%m%d") - timedelta(days_ago) ).strftime("%Y%m%d")
                device_groups_path1 = device_groups_path + d
                
                dfdg = self.spark.read.parquet(device_groups_path1)\
                                        .select("rowkey",explode("Group_Data_sys_info"))\
                                        .transform(flatten_df_v2)\
                                        .transform(DG_process)
                break
            except:
                pass
                                
        cond = [dfdg.dg_rowkey==df_all.rk_row_sn, dfdg.RouExt_mac==df_all.parent_id]
        df_mac =  dfdg.join(df_all,cond,"right")\
                        .withColumn("Rou_Ext",when( col("parent_mac").isNotNull(),1 ).otherwise(0) )
    
        #group_id = ["dg_rowkey", "serial_num", "mdn", "cust_id", "rowkey", "rk_row_sn", "station_mac"] 
        group_id = ["serial_num", "mdn", "cust_id", "rowkey", "rk_row_sn", "station_mac"] 
        rou_ext = ["RouExt_mac", "dg_model", "parent_mac", "firmware", "parent_id", "Rou_Ext"] 
        features = ["avg_phyrate", "poor_phyrate", "stationarity", "weights", "poor_rssi"] 
        
        aggregation_exprs = [F.avg(col(feature)).alias(feature) for feature in features] + \
                            [F.collect_set(col(col_name)).alias(col_name) for col_name in rou_ext]
        
        result_df = df_mac.groupBy(group_id).agg(*aggregation_exprs) 
        
        return result_df
            

        
if __name__ == "__main__":
    
    spark = SparkSession.builder\
            .master("spark://njbbepapa1.nss.vzwnet.com:7077") \
            .appName('wifiScore_ZheS')\
            .config("spark.sql.adapative.enabled","true")\
            .enableHiveSupport().getOrCreate()
    #
    hdfs_pd = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/"
    mail_sender = MailSender() 
    datetoday = date.today() 
    #datetoday = date(2024,1,27)

    try:
        ins1 = wifiScore(  
                        sparksession = spark,
                        day = datetoday
                        )

        df_deviceScore = ins1.df_deviceScore
        df_deviceScore = round_columns(df_deviceScore,["avg_phyrate","poor_phyrate","poor_rssi","device_score"], 2)
        df_deviceScore = round_columns(df_deviceScore,["weights"], 4)

        df_deviceScore.dropDuplicates()\
                .withColumn("date", F.lit((datetoday - timedelta(1)).strftime('%Y-%m-%d')))\
                .repartition(100)\
                .write.mode("overwrite")\
                .parquet( hdfs_pd + "/user/ZheS/wifi_score_v2/deviceScore_dataframe/" + (datetoday - timedelta(1)).strftime("%Y-%m-%d") )
                
        ins1.df_homeScore.repartition(10)\
                .withColumn("date", F.lit((datetoday - timedelta(1)).strftime('%Y-%m-%d')))\
                .write.mode("overwrite")\
                .parquet( hdfs_pd + "/user/ZheS/wifi_score_v2/homeScore_dataframe/" + (datetoday- timedelta(1)).strftime("%Y-%m-%d") )
            

    except Exception as e:
        mail_sender.send(text = e, subject="wifiScore_ZheS failed")