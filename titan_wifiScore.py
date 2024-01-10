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

def send_mail_base(send_from, send_to, subject, cc ,text, files=None, server='vzsmtp.verizon.com' ): 
    assert isinstance(send_to, list) 
    assert isinstance(cc, list) 
    
    msg = MIMEMultipart() 
    msg['From'] = send_from 
    msg['To'] = COMMASPACE.join(send_to) 
    msg['Cc'] = COMMASPACE.join(cc) 
    msg['Date'] = formatdate(localtime=True) 
    msg['Subject'] = subject 

    # Set the text with inline CSS for dark black color  
    text_with_style = f'<div style="color: #100000;">{text}</div>'  
    msg.attach(MIMEText(text_with_style, 'html'))  

    for f in files or []: 
        with open(f, "rb") as fp: 
            attachment = MIMEBase('application', "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet") 
            attachment.set_payload(fp.read()) 
            encoders.encode_base64(attachment) 
            attachment.add_header('Content-Disposition', 'attachment', filename=os.path.basename(report_file_name)) 
            msg.attach(attachment) 

    smtp = smtplib.SMTP(server) 
    smtp.sendmail(send_from, send_to + cc, msg.as_string()) 
    smtp.close()

def process_csv_files(date_range, file_path_pattern): 

    """ 
    Reads CSV files from HDFS for the given date range and file path pattern and processes them.
    Args: 
        date_range (list): List of date strings in the format 'YYYY-MM-DD'. 
        file_path_pattern (str): File path pattern with a placeholder for the date, e.g., "/user/.../{}.csv"
    Returns: 
        list: A list of processed PySpark DataFrames. 
    """ 
    
    df_list = [] 
    for d in date_range: 
        file_path = file_path_pattern.format(d) 
        try:
            df = spark.read.option("recursiveFileLookup", "true").option("header", "true").csv(file_path)
            df_list.append(df)
        except:
            print(f"data missing at {file_path}")
    return df_list 

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
    spark = SparkSession.builder.appName('titan_wifiScore_ZheS').enableHiveSupport().getOrCreate()
    hdfs_pd = 'hdfs://njbbvmaspd11.nss.vzwnet.com:9000'

    models = ['ASK-NCQ1338', 'ASK-NCQ1338FA', 'ASK-NCQ1338E', 'XCI55AX', 'CR1000A', 'FSNO21VA', 'ASK-NCM1100E','WNC-CR200A']
    
    date_list = [(date.today() - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(1, 8)]

    for date_str in date_list:

        path = hdfs_pd + f"/user/ZheS/wifi_score_v2/homeScore_dataframe/{date_str}"
        dfsh_all_Hsc = spark.read.parquet(path)\
                            .withColumn("dg_model",F.explode("dg_model") )\
                            .withColumn("dg_model_indiv",F.explode("dg_model") )
        df_vmb = dfsh_all_Hsc.filter( col("dg_model_indiv").isin(models) )
        
        mdn_list = ["9038642801", "2148469941", "4699346290", "2792302789", "2393852612", 
                    "4809524756", "5018042955", "5018132107", "7252742413", "6092260621", 
                    "7734851554", "3033455207", "4805210693", "6892226653", "9735294196", 
                    "5136670854", "6572779449", "6783370432", "7252733733", "3606189834", 
                    "6822746562", "9842869397", "8016410148", "6822706366", "7079788420", 
                    "7792381820"] 
        
        filtered_df = df_vmb.filter(F.col("mdn").isin(mdn_list)) 
        
        output_path = f"/user/ZheS/wifi_score_v2/titan_3_WNC-CR200A/{date_str}.csv"
        filtered_df.drop("dg_model","Rou_Ext")\
                        .repartition(1)\
                        .write.format("csv")\
                        .mode("overwrite")\
                        .option("header", "true")\
                        .save(output_path)

    file_path_pattern = hdfs_pd + "/user/ZheS/wifi_score_v2/titan_3_WNC-CR200A/{}.csv"

    df_list = process_csv_files(date_list, file_path_pattern)
    df_union = union_df_list(df_list)

    pd_df = df_union.toPandas()

    report_file_name = f'{date_list[-1]}_{date_list[0]}.xlsx' 

    writer = pd.ExcelWriter(report_file_name) 

    pd_df.to_excel(writer) 

    writer.close() 
    
    send_mail_base("zhe.sun@verizonwireless.com", ["zhe.sun@verizonwireless.com"], "excel file",[] ,'text', files=[report_file_name], server='vzsmtp.verizon.com' ) 