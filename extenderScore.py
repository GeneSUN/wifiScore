import pandas as pd 
import numpy as np 
import seaborn as sns 
import matplotlib.pyplot as plt  
from sklearn.model_selection import train_test_split  
from sklearn.ensemble import RandomForestClassifier  
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix  
from datetime import datetime, timedelta, date
from functools import reduce
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank, sum, concat_ws, col, split, concat_ws, lit ,udf,count, max,lit,avg, when,concat_ws,to_date,explode
from pyspark.sql.functions import col, to_date, month 
from pyspark.sql import functions as F 
from pyspark.sql.functions import (collect_list,concat,from_unixtime,lpad, broadcast, sum, udf, col, abs, length, min, max, lit, avg, when, concat_ws, to_date, exp, explode,countDistinct, first,round  ) 
from pyspark.sql import SparkSession 
from pyspark.sql.types import FloatType

import numpy as np

from functools import reduce 
import sys


from IPython.display import display
from tabulate import tabulate

    
if __name__ == "__main__":
    
    spark = SparkSession.builder\
            .appName('testScript_ZheS')\
            .config("spark.sql.adapative.enabled","true")\
            .getOrCreate()
    
    hdfs_pd = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/"
    p = hdfs_pd +  "/user/ZheS/wifi_score_v3/installExtenders/"

    features = ["poor_phyrate","poor_rssi",
            "avg_phyrate","avg_sig_strength_cat1","avg_sig_strength_cat2",
            "before_home_score","count",
            "volume"]

    routers = ["G3100","CR1000A","XCI55AX","ASK-NCQ1338FA","CR1000B","WNC-CR200A"]
    other_routers = ["FWA55V5L","FWF100V5L","ASK-NCM1100E","ASK-NCM1100","ASK-NCQ1338","FSNO21VA","ASK-NCQ1338E"]
    extender = ["E3200","CE1000A","CME1000"]

    result_df = ( 
                    spark.read.option("recursiveFileLookup", "true").parquet(p)\
                        .filter(col("dg_model_indiv").isin(routers + other_routers) )\
                        .withColumn("dg_model_indiv", 
                                    when(col("dg_model_indiv").isin(other_routers), "others")
                                    .otherwise(col("dg_model_indiv")))\
                        .filter( col("before_home_score") > 50)\
                        .withColumn("score_increment", F.round(col("after_home_score")-col("before_home_score"),4) )\
                        .select( features + ["score_increment"] )\
                        .withColumn("enhance_flag", when( col("score_increment")> 0, 1).otherwise(0))\
                )
    
    dataset_comb_0 = result_df.drop("score_increment")\
                           .toPandas()

    columns_to_fill = ["avg_sig_strength_cat1","avg_sig_strength_cat2"]
    dataset_comb_0[columns_to_fill] = dataset_comb_0[columns_to_fill].fillna(dataset_comb_0[columns_to_fill].mean())
    dataset_comb_0[columns_to_fill].mean()

    df = dataset_comb_0 
    X = df[features]  
    y = df['enhance_flag']  
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)  

    weight_for_class_0 = len(y_train) / (2 * (len(y_train) -y_train.sum())) 
    weight_for_class_1 = len(y_train) / (2 * y_train.sum()) 
    class_weight = {0: weight_for_class_0, 1: weight_for_class_1}

    rf_model = RandomForestClassifier( 
                                        n_estimators=50,  
                                        max_depth=5,  
                                        min_samples_split=10,  
                                        min_samples_leaf=5,  
                                        bootstrap=True,  
                                        random_state=42,
                                        n_jobs = -1,
                                        class_weight = class_weight
                                    ) 
    rf_model.fit(X_train, y_train)  

    y_test_pred = rf_model.predict(X_test)  
    accuracy_test = accuracy_score(y_test, y_test_pred)  
    conf_matrix_test = confusion_matrix(y_test, y_test_pred)  
    classification_rep_test = classification_report(y_test, y_test_pred)  
    
    print("\nTest Set Metrics:") 
    print(f"Accuracy: {accuracy_test:.2f}")  
    print("\nConfusion Matrix:")  
    print(conf_matrix_test)  
    print("\nClassification Report:")  
    print(classification_rep_test)  
    print("Recall: the percentage of predicted positive among all true positive") 

    print( "Recall: Profit",  )
    print("For class improved , among all actual improved, {} were correctly predicted as improved. ".format(np.round( conf_matrix_test[1][1]/(conf_matrix_test[1][0]+conf_matrix_test[1][1]), 2)))

    print( "Precision: Reverse Cost")
    print("For class improved , among all instances predicted as improved, {} were actually improved. ".format(np.round( conf_matrix_test[1][1]/(conf_matrix_test[0][1]+conf_matrix_test[1][1]), 2)))

    # deployment
    features = ["poor_phyrate","poor_rssi",
            "avg_phyrate","avg_sig_strength_cat1","avg_sig_strength_cat2",
            "before_home_score","count",
            "volume"]
    
    date_string = (date.today() - timedelta(1)).strftime('%Y-%m-%d') 
    df_pop = spark.read.parquet( hdfs_pd + f"/user/ZheS/wifi_score_v3/router_dataset_Model_deploy/router_{date_string}_window_range_7")\
                    .filter(col("before_home_score")<100)\
                    .select( ["serial_num"] + features)

    pandas_pop = df_pop.toPandas()

    pandas_pop = pandas_pop.dropna(subset=['before_home_score', 'volume']) 
    pandas_pop = pandas_pop.fillna({'avg_sig_strength_cat1': -60, 'avg_sig_strength_cat2': -65}) 

    y_pred_probs = rf_model.predict_proba(pandas_pop[features])[:,1]
    pred_proba_df = pd.DataFrame(data = y_pred_probs, columns = ["extender_score"] )
    pandas_extenderScore = pd.concat( [pandas_pop.reset_index(drop = True), pred_proba_df], axis = 1 )

    df_join = spark.createDataFrame( pandas_extenderScore[["serial_num","extender_score"]] )
    df_extenderScore = df_pop.join( df_join, "serial_num")

    df_extenderScore.withColumn("extender_score",F.round("extender_score",4 ))\
                .write.mode("overwrite").parquet(hdfs_pd + "/user/ZheS/wifi_score_v3/extenderScore_dataframe/" + date_string)