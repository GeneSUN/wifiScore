from pyspark.sql import functions as F 
from pyspark.sql.functions import (concat,from_unixtime,lpad, broadcast, sum, udf, col, abs, length, min, max, lit, avg, when, concat_ws, to_date, exp, explode,countDistinct, first,round  ) 
from pyspark.sql import SparkSession 
from datetime import datetime, timedelta, date 
from dateutil.parser import parse
import argparse 

import smtplib
from email.mime.text import MIMEText
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.utils import COMMASPACE, formatdate
import os

if __name__ == "__main__":
    # the only input is the date which is used to generate 'date_range'
    spark = SparkSession.builder.appName('titan_wifiScore_ZheS').enableHiveSupport().getOrCreate()
    
    models = ['ASK-NCQ1338', 'ASK-NCQ1338FA', 'ASK-NCQ1338E', 'XCI55AX', 'CR1000A', 'FSNO21VA', 'ASK-NCM1100E','WNC-CR200A']
    start_date = datetime(2023, 11, 18); end_date = datetime(2023, 12, 31) 
    date_list = [ (start_date + timedelta(days=x)).strftime("%Y-%m-%d") for x in range((end_date - start_date).days + 1)] 

    for date_str in date_list:
        hdfs_title = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/"
        path = hdfs_title + f"/user/maggie/temp1.1/wifiScore_detail{date_str}.json"
        
        try:
                    
            df_temp = spark.read.json(path)
            
            dfsh_all_Hsc = df_temp.groupby("serial_num","mdn","cust_id")\
                                                .agg(
                                                F.round(sum(df_temp.poor_rssi*df_temp.weights),4).alias("poor_rssi"),\
                                                F.round(sum(df_temp.poor_phyrate*df_temp.weights),4).alias("poor_phyrate"),\
                                                F.round(sum(df_temp.score*df_temp.weights),4).alias("home_score"),\
                                                max("dg_model").alias("dg_model"),\
                                                max("date").alias("date"),\
                                                max("firmware").alias("firmware")
                                                )
            df_vmb = dfsh_all_Hsc.select( "*",F.explode("dg_model").alias("dg_model_indiv") ).filter( col("dg_model_indiv").isin(models) )
            
            try:
                p = f"/usr/apps/vmas/5g_data/fixed_5g_router_mac_sn_mapping/{date_str}/fixed_5g_router_mac_sn_mapping.csv"
                df_join = spark.read.option("header","true").csv(p)\
                                .select( col("mdn_5g").alias("mdn"),
                                        col("serialnumber").alias("serial_num"),
                                        "cust_id"
                                        )
            except:
                p = "/usr/apps/vmas/5g_data/fixed_5g_router_mac_sn_mapping/2023-12-14/fixed_5g_router_mac_sn_mapping.csv"
                df_join = spark.read.option("header","true").csv(p)\
                                .select( col("mdn_5g").alias("mdn"),
                                        col("serialnumber").alias("serial_num"),
                                        "cust_id"
                                        )
                                    
            df1 = df_vmb.drop("mdn","cust_id")
            joined_df = df1.join(df_join,"serial_num", "inner")
            
            mdn_list = ["9038642801", "2148469941", "4699346290", "2792302789", "2393852612", 
                        "4809524756", "5018042955", "5018132107", "7252742413", "6092260621", 
                        "7734851554", "3033455207", "4805210693", "6892226653", "9735294196", 
                        "5136670854", "6572779449", "6783370432", "7252733733", "3606189834", 
                        "6822746562", "9842869397", "8016410148", "6822706366", "7079788420", 
                        "7792381820"] 
            
            
            
            # Filter rows based on mdn_list 
            filtered_df = joined_df.filter(F.col("mdn").isin(mdn_list)) 
            
            output_path = f"/user/ZheS/wifi_score_v2/titan_3_WNC-CR200A/{date_str}.csv"
            filtered_df.drop("dg_model")\
                            .repartition(1)\
                            .write.format("csv")\
                            .mode("overwrite")\
                            .option("header", "true")\
                            .save(output_path)
        except Exception as e:
            print(date_str, f"An error occurred: {e}")