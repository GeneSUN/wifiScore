
from pyspark.sql import functions as F 
from pyspark.sql.functions import (concat,from_unixtime,lpad, broadcast, sum, udf, col, abs, length, min, max, lit, avg, when, concat_ws, to_date, exp, explode,countDistinct, first,round  ) 
from pyspark.sql import SparkSession 
from datetime import datetime, timedelta, date 
from pyspark.sql import functions as F 
from pyspark.sql.functions import (concat,from_unixtime,lpad, broadcast, sum, udf, col, abs, length, min, max, lit, avg, when, concat_ws, to_date, exp, explode,countDistinct, first,round  ) 

import pandas as pd  
import numpy as np
from sklearn.model_selection import train_test_split  
from sklearn.ensemble import RandomForestClassifier  
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix  
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

def process_csv_files(date_range, file_path_pattern, func = None): 

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
            df_kpis = spark.read.json(file_path)
            
            if func is not None:
                df_kpis = func(df_kpis)
            
            df_list.append(df_kpis)
        except Exception as e:
            print(e)
            #print(f"data missing at {file_path}")

    return df_list

if __name__ == "__main__":
    # the only input is the date which is used to generate 'date_range'
    spark = SparkSession.builder.appName('ZheS_wifiscore_preprocess')\
                        .config("spark.sql.adapative.enabled","true")\
                        .getOrCreate()
    hdfs_pd = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/"

    def func(path_window,test_path):
            p = hdfs_pd +  "/user/ZheS/wifi_score_v2/training_dataset/"+path_window
            result_df = spark.read.parquet(p)\
                            .withColumn("score_increment", F.round(col("after_home_score")-col("before_home_score"),4) )\
                            .select("avg_phyrate","poor_phyrate","poor_rssi","count","score_increment")
            result_df.count()
            feature_columns = [e for e in result_df.columns if e not in ["serial_num","score_increment","median_score"] ]  # Exclude "serial_num" and "home_score" 
            dataset_comb = result_df.withColumn("enhance_flag", when( col("score_increment")> 0, 1).otherwise(0)).drop("score_increment").toPandas()

            import matplotlib.pyplot as plt
            count_by_flag = dataset_comb['enhance_flag'].value_counts() 
            print(count_by_flag)

            df = dataset_comb 
            X = df[['avg_phyrate', 'poor_phyrate', 'poor_rssi', 'count']]  
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

            # Evaluate the model on the test set  
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
            #------------------------------------------------------------------------------------------------------------------

            feature_columns = [e for e in result_df.columns if e not in ["serial_num","score_increment","median_score"] ]  # Exclude "serial_num" and "home_score" 
            dataset_comb_5= result_df.withColumn("enhance_flag", when( col("score_increment")> 5, 1).otherwise(0)).drop("score_increment").toPandas() 

            import matplotlib.pyplot as plt 
            count_by_flag = dataset_comb_5['enhance_flag'].value_counts() 


            df = dataset_comb_5 
            X = df[['avg_phyrate', 'poor_phyrate', 'poor_rssi', 'count']]  
            y = df['enhance_flag']  
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)  

            weight_for_class_0 = len(y_train) / (2 * (len(y_train) -y_train.sum())) 
            weight_for_class_1 = len(y_train) / (2 * y_train.sum()) 
            class_weight = {0: weight_for_class_0, 1: weight_for_class_1}

            rf_model_5 = RandomForestClassifier( 
                                                n_estimators=50,  
                                                max_depth=5,  
                                                min_samples_split=10,  
                                                min_samples_leaf=5,  
                                                bootstrap=True,  
                                                random_state=42,
                                                n_jobs = -1,
                                                class_weight = class_weight
                                            ) 
            rf_model_5.fit(X_train, y_train)  

            # Evaluate the model on the test set  
            y_test_pred = rf_model_5.predict(X_test)  
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

            #------------------------------------------------------------------------------------------------------------------

            feature_columns = [e for e in result_df.columns if e not in ["serial_num","score_increment","median_score"] ]  # Exclude "serial_num" and "home_score" 
            dataset_comb = result_df.withColumn("enhance_flag", when( col("score_increment")> 10, 1).otherwise(0)).drop("score_increment").toPandas() 

            df = dataset_comb 
            X = df[['avg_phyrate', 'poor_phyrate', 'poor_rssi', 'count']]  
            y = df['enhance_flag']  
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)  

            weight_for_class_0 = len(y_train) / (2 * (len(y_train) -y_train.sum())) 
            weight_for_class_1 = len(y_train) / (2 * y_train.sum()) 
            class_weight = {0: weight_for_class_0, 1: weight_for_class_1}

            rf_model_10 = RandomForestClassifier( 
                                                n_estimators=50,  
                                                max_depth=5,  
                                                min_samples_split=10,  
                                                min_samples_leaf=5,  
                                                bootstrap=True,  
                                                random_state=42,
                                                n_jobs = -1,
                                                class_weight = class_weight
                                            ) 
            rf_model_10.fit(X_train, y_train)  

            # Evaluate the model on the test set  
            y_test_pred = rf_model_10.predict(X_test)  
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

            #------------------------------------------------------------------------------------------------------------------

            p = test_path
            router_df = spark.read.parquet(p)
            df = router_df.toPandas() 
            X_valid = df[['avg_phyrate', 'poor_phyrate', 'poor_rssi', 'count']]  

            y_valid_pred = rf_model.predict(X_valid) 

            # Count the number of zeros and ones 
            count_zeros = np.count_nonzero(y_valid_pred == 0) 
            count_ones = np.count_nonzero(y_valid_pred == 1) 

            print(f"Number of zeros: {count_zeros}") 
            print(f"Number of ones: {count_ones}")
            print(f"Percentage of potential enhancement: { np.round( count_ones/(count_ones+count_zeros)*100,2 )}%") 

            y_valid_pred = rf_model_5.predict(X_valid) 

            # Count the number of zeros and ones 
            count_zeros = np.count_nonzero(y_valid_pred == 0) 
            count_ones = np.count_nonzero(y_valid_pred == 1) 

            print(f"Number of zeros: {count_zeros}") 
            print(f"Number of ones: {count_ones}")
            print(f"Percentage of potential enhancement: { np.round( count_ones/(count_ones+count_zeros)*100,2 )}%") 

            y_valid_pred = rf_model_10.predict(X_valid) 

            # Count the number of zeros and ones 
            count_zeros = np.count_nonzero(y_valid_pred == 0) 
            count_ones = np.count_nonzero(y_valid_pred == 1) 

            print(f"Number of zeros: {count_zeros}") 
            print(f"Number of ones: {count_ones}")
            print(f"Percentage of potential enhancement: { np.round( count_ones/(count_ones+count_zeros)*100,2 )}%") 


    p7_list = ["2023-09-24_2023-10-01_2023-10-08_window_range_7",
            "2023-10-08_2023-10-15_2023-10-22_window_range_7",
            "2023-10-15_2023-10-22_2023-10-29_window_range_7",
            "2023-10-22_2023-10-29_2023-11-05_window_range_7"
            ]
    p3_list = [
        "2023-09-28_2023-10-01_2023-10-04_window_range_3",
        "2023-10-01_2023-10-04_2023-10-07_window_range_3",
        "2023-10-04_2023-10-07_2023-10-10_window_range_3",
        "2023-10-07_2023-10-10_2023-10-13_window_range_3",
        "2023-10-10_2023-10-13_2023-10-16_window_range_3",
        "2023-10-13_2023-10-16_2023-10-19_window_range_3",
        "2023-10-16_2023-10-19_2023-10-22_window_range_3",
        "2023-10-19_2023-10-22_2023-10-25_window_range_3",
        "2023-10-22_2023-10-25_2023-10-28_window_range_3",
        "2023-10-25_2023-10-28_2023-10-31_window_range_3",
        ]
    p14_list = ["2023-09-17_2023-10-01_2023-10-15_window_range_14",
                "2023-10-01_2023-10-15_2023-10-29_window_range_14",
                "2023-10-15_2023-10-29_2023-11-12_window_range_14"
                ]
    p30_list = ["2023-09-01_2023-10-01_2023-10-31_window_range_30"]
    test_path = hdfs_pd +  "//user/ZheS/wifi_score_v2/training_dataset/router_2023-10-01_window_range_7"
    for pp in p14_list:
        func(pp,test_path)