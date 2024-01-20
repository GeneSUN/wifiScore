from datetime import datetime, timedelta, date
from functools import reduce
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank, sum, concat_ws, col, split, concat_ws, lit ,udf,count, max,lit,avg, when,concat_ws,to_date,explode

from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import mgrs
from pyspark.sql.types import (DateType, DoubleType, StringType, StructType, StructField) 
import sys 
sys.path.append('/usr/apps/vmas/script/ZS/SNAP') 
sys.path.append('/usr/apps/vmas/script/ZS') 
from MailSender import MailSender

def process_csv_files(date_range, file_path_pattern, dropduplicate=False):  
    """   
    Reads CSV files from HDFS for the given date range and file path pattern and union them.  
    Args:   
        date_range (list): List of date strings in the format 'YYYY-MM-DD'.   
        file_path_pattern (str): File path pattern with a placeholder for the date, e.g., "/user/.../{}.csv"  
        dropduplicate (bool): Whether to drop duplicate rows (default is False).  
    Returns:   
        pyspark.sql.DataFrame: A unioned DataFrame of processed PySpark DataFrames.  
    """   

    def process_csv(date):  
        file_path = file_path_pattern.format(date)
        df_kpis = spark.read.parquet(file_path)  
        if dropduplicate:  
            df_kpis = df_kpis.dropDuplicates()  
        return df_kpis  
    df_list = map(process_csv, date_range)  
    return reduce(lambda df1, df2: df1.union(df2), df_list)  
if __name__ == "__main__":
    spark = SparkSession.builder\
        .appName('ZheS_TrueCall')\
        .master("spark://njbbepapa1.nss.vzwnet.com:7077")\
        .config("spark.sql.adapative.enabled","true")\
        .getOrCreate()
    mail_sender = MailSender() 
    hdfs_pd = 'hdfs://njbbvmaspd11.nss.vzwnet.com:9000'
    #-----------------------------------------------------------------------

    from functools import reduce  

    
    def func(date_list):
        order_list = ["ASK-NCQ1338", "ASK-NCQ1338FA", "XCI55AX", "WNC-CR200A"] 
        df_wifi = process_csv_files( date_range = date_list, 
                                    file_path_pattern = hdfs_pd+"/user/ZheS/wifi_score_v2/homeScore_dataframe/{}" 
                                    )\
                                    .withColumn("dg_model", F.explode("dg_model"))\
                                    .withColumn("dg_model", F.explode("dg_model"))\
                                    .dropDuplicates()
        df_result = df_wifi.groupby("dg_model","date").agg( F.round(avg("home_score"),2).alias("home_score"),count("*").alias("count")  )\
                            .filter(col("dg_model").isin(order_list))
        df_result.orderBy("dg_model").show()
        #ordered_df = df_result.orderBy(F.expr("FIELD(dg_model, {})".format(str(order_list))), ascending=True) 
        #ordered_df.show()
    
    func(["2024-01-01"])
    func(["2024-01-04"])
    func(["2024-01-10"])
    func(["2024-01-15"])
    sys.exit()
