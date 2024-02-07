
from datetime import datetime, timedelta, date 
from pyspark.sql import functions as F 
from pyspark.sql.functions import (concat,from_unixtime,lpad, broadcast, sum, udf, col, abs, length, min, max, lit, avg, when, concat_ws, to_date, exp, explode,countDistinct, first,round  ) 
from pyspark.sql import SparkSession 
import pandas as pd 
import seaborn as sns 
import matplotlib.pyplot as plt 
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

def plot_feature_score(result_df, feature_name, bins, threshold, is_greater = True, score_range =[-20, 20] ):
    feature_columns = [e for e in result_df.columns if e not in ["serial_num","score_increment"] ]  # Exclude "serial_num" and "home_score" 
    pandas_df = result_df.select(['score_increment'] + feature_columns ).toPandas() 
    
    pandas_df[feature_name].describe()
    
    #bins = [0, 2, 4, 6 ,10, 20, 30, 100] 
    pandas_df[f"{feature_name}_range"] = pd.cut(pandas_df[feature_name], bins=bins, right=False)
    
    result = pandas_df.groupby( f"{feature_name}_range").agg({'score_increment': lambda x: (x >= 0).mean() * 100, 
                                            'count': 'count'}).reset_index() 
    result.rename(columns={'score_increment': 'score_increment_pos'}, inplace=True) 
    print( "")
    print( result )
    print( "")
    
    plt.figure(figsize=(12, 6)) 
    
    plt.subplot(1, 2, 1) 
    pandas_df['score_increment'].hist(range=[-20, 20]) 
    plt.xlabel('score_increment') 
    plt.ylabel("count") 
    plt.title('Distribution of score_increment among All Data') 
    
    plt.subplot(1, 2, 2) 
    #plt.subplot(1, 2, 2, sharey=plt.gca()) 
    if is_greater:
        pandas_df[pandas_df[feature_name] > threshold]['score_increment'].hist(range=score_range) 
        plt.title(f'Distribution of score_increment among {feature_name} above {threshold}') 
    else:
        pandas_df[pandas_df[feature_name] < threshold]['score_increment'].hist(range=score_range) 
        plt.title(f'Distribution of score_increment among {feature_name} below {threshold}') 
    plt.xlabel('score_increment') 
    plt.ylabel('count') 
    
    
    plt.tight_layout() 
    plt.show()
    print()


if __name__ == "__main__":
    # the only input is the date which is used to generate 'date_range'
    spark = SparkSession.builder.appName('ZheS_wifiscore_preprocess')\
                        .config("spark.sql.adapative.enabled","true")\
                        .getOrCreate()
    hdfs_pd = 'hdfs://njbbvmaspd11.nss.vzwnet.com:9000/'

    hdfs_pd = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/"

    p = hdfs_pd +  "/user/ZheS/wifi_score_v2/training_dataset/2023-09-24_2023-10-01_2023-10-08_window_range_7"
    result_df = spark.read.parquet(p)\
                    .withColumn("score_increment", F.round(col("target_score")-col("score"),4) )\
                    .select("avg_phyrate","poor_phyrate","poor_rssi","count","score_increment")
    result_df.count()
    plot_feature_score(result_df,"avg_phyrate",[0, 200, 300 ,400, 500, 2400] , 400, is_greater = False)
    plot_feature_score("poor_phyrate",[0, 2, 4, 6 ,10, 20, 30, 100] ,10)
    plot_feature_score("poor_rssi",[0, 10, 20, 30, 40, 50, 60, 70, 100] ,30)
    plot_feature_score("count", [0, 100, 200, 300, 400, 500, 600, 700, 2000]  ,200)

    import numpy as np
    from pyspark.sql.functions import sum, concat_ws, col, split, concat_ws, lit ,udf,count, max,lit,avg, when,concat_ws,to_date,explode

    filtered_df = result_df.filter((col('avg_phyrate') < 400) & (col('poor_phyrate') > 6) & (col('poor_rssi') > 20) & (col('count') > 200)) 

    percentage_high_score_increment = (filtered_df.filter(col('score_increment') > 0).count() / filtered_df.count()) * 100 

    print(
            f"Number of homes installed extender: {result_df.count()}", 
            f"\nNumber of extender homes predict enhancement : {filtered_df.count()}", 
            f"\nPercentage of predicted enhancement (score_increment > 0): {np.round(filtered_df.count()/result_df.count()*100,2)}%",
            f"\nPercentage of True predicted enhancement: {np.round(percentage_high_score_increment,2)}%"
    ) 

    print( f"\nPredicted of enhancement distribution")
    increment_ranges = [-20, 0, 10, 20, 30, 50, 70] 
    df = filtered_df.withColumn( 
        'range_label', 
        when((col('score_increment') >= increment_ranges[0]) & (col('score_increment') < increment_ranges[1]), f'[{increment_ranges[0]}, {increment_ranges[1]})') \
        .when((col('score_increment') >= increment_ranges[1]) & (col('score_increment') < increment_ranges[2]), f'[{increment_ranges[1]}, {increment_ranges[2]})') \
        .when((col('score_increment') >= increment_ranges[2]) & (col('score_increment') < increment_ranges[3]), f'[{increment_ranges[2]}, {increment_ranges[3]})') \
        .when((col('score_increment') >= increment_ranges[3]) & (col('score_increment') < increment_ranges[4]), f'[{increment_ranges[3]}, {increment_ranges[4]})') \
        .when((col('score_increment') >= increment_ranges[4]) & (col('score_increment') < increment_ranges[5]), f'[{increment_ranges[4]}, {increment_ranges[5]})') \
        .when((col('score_increment') >= increment_ranges[5]) & (col('score_increment') <= increment_ranges[6]), f'[{increment_ranges[5]}, {increment_ranges[6]}]'))\
        .dropna()

    statistics_table = df.groupBy('range_label').agg( 
        count('score_increment').alias('count'), 
        F.round( (count('score_increment') / df.count() * 100),2).alias('percentage_frequency') 
    ) 

    statistics_table.orderBy("range_label").show()

    import pandas as pd 
    import seaborn as sns 
    import matplotlib.pyplot as plt 
    hist_params = { 
        'column': 'score_increment', 
        'range': [-20, 50], 
        'bins': 20 
    } 

    filtered_data = df.filter((col(hist_params['column']) >= hist_params['range'][0]) & (col(hist_params['column']) <= hist_params['range'][1])) 

    pandas_df = filtered_data.select(hist_params['column']).toPandas() 

    plt.hist(pandas_df[hist_params['column']], bins=hist_params['bins'], range=hist_params['range'], edgecolor='black') 
    plt.xlabel('Score Increment') 
    plt.ylabel('Counr') 
    plt.title('Histogram of Score Increments') 
    plt.show() 

    feature_columns = [e for e in result_df.columns if e not in ["serial_num","score_increment","median_score"] ]  # Exclude "serial_num" and "home_score" 
    dataset_comb = result_df.withColumn("enhance_flag", when( col("score_increment")> 0, 1).otherwise(0)).drop("score_increment").toPandas()

    import matplotlib.pyplot as plt
    count_by_flag = dataset_comb['enhance_flag'].value_counts() 
    ax = count_by_flag.plot(kind='bar', color=['#D8C4FB', '#C2DFFF']) 
    plt.title('Count of enhance_flag Values')
    plt.xlabel('enhance_flag') 
    plt.ylabel('Count') 
    plt.xticks(rotation=0) 
    for i, count in enumerate(count_by_flag): 
        ax.text(i, count + 0.1, str(count), ha='center', va='bottom', fontsize=8) 
    plt.show() 

    print(count_by_flag)

    #---------------------------------------------------------------------
    import pandas as pd 
    from sklearn.model_selection import train_test_split  
    from sklearn.linear_model import LogisticRegression  
    from sklearn.metrics import accuracy_score, classification_report, confusion_matrix  
    
    df = dataset_comb 
    X = df[['avg_phyrate', 'poor_phyrate', 'poor_rssi', 'count']]  
    y = df['enhance_flag']  

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)  

    weight_for_class_0 = len(y_train) / (2 * (len(y_train) -y_train.sum())) 
    weight_for_class_1 = len(y_train) / (2 * y_train.sum()) 
    class_weight = {0: weight_for_class_0, 1: weight_for_class_1}

    model = LogisticRegression(class_weight = class_weight)  
    model.fit(X_train, y_train)  

    # Make predictions on the training set 
    y_train_pred = model.predict(X_train) 

    # Evaluate the model on the training set 
    accuracy_train = accuracy_score(y_train, y_train_pred) 
    conf_matrix_train = confusion_matrix(y_train, y_train_pred) 
    classification_rep_train = classification_report(y_train, y_train_pred) 


    # Evaluate the model on the test set  
    y_test_pred = model.predict(X_test)  
    accuracy_test = accuracy_score(y_test, y_test_pred)  
    conf_matrix_test = confusion_matrix(y_test, y_test_pred)  
    classification_rep_test = classification_report(y_test, y_test_pred)  

    print("Training Set Metrics:") 
    print(f"Accuracy: {accuracy_train:.2f}")  
    print("Confusion Matrix:")  
    print(conf_matrix_train)  
    print("Classification Report:")  
    print(classification_rep_train)  
    
    print("\nTest Set Metrics:")
    print(f"Accuracy: {accuracy_test:.2f}")  
    print("Confusion Matrix:")  
    print(conf_matrix_test)  
    print("Classification Report:")  
    print(classification_rep_test)  
    print("Recall: the percentage of predicted positive among all true positive") 


    #---------------------------------------------------------------------
    import pandas as pd  
    from sklearn.model_selection import train_test_split  
    from sklearn.ensemble import RandomForestClassifier  
    from sklearn.metrics import accuracy_score, classification_report, confusion_matrix  
    
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

    # Evaluate the model on the training set 
    y_train_pred = rf_model.predict(X_train) 
    accuracy_train = accuracy_score(y_train, y_train_pred) 
    conf_matrix_train = confusion_matrix(y_train, y_train_pred) 
    classification_rep_train = classification_report(y_train, y_train_pred) 

    # Evaluate the model on the test set  
    y_test_pred = rf_model.predict(X_test)  
    accuracy_test = accuracy_score(y_test, y_test_pred)  
    conf_matrix_test = confusion_matrix(y_test, y_test_pred)  
    classification_rep_test = classification_report(y_test, y_test_pred)  

    print("Training Set Metrics:") 
    print(f"Accuracy: {accuracy_train:.2f}")  
    print("\nConfusion Matrix:")  
    print(conf_matrix_train)  
    print("\nClassification Report:")  
    print(classification_rep_train)  
    
    print("\nTest Set Metrics:") 
    print(f"Accuracy: {accuracy_test:.2f}")  
    print("\nConfusion Matrix:")  
    print(conf_matrix_test)  
    print("\nClassification Report:")  
    print(classification_rep_test)  
    print("Recall: the percentage of predicted positive among all true positive") 
    #---------------------------------------------------------------------

    feature_columns = [e for e in result_df.columns if e not in ["serial_num","score_increment","median_score"] ]  # Exclude "serial_num" and "home_score" 
    dataset_comb = result_df.withColumn("enhance_flag", when( col("score_increment")> 5, 1).otherwise(0)).drop("score_increment").toPandas() 

    import matplotlib.pyplot as plt 
    count_by_flag = dataset_comb['enhance_flag'].value_counts() 
    print(count_by_flag)
    #---------------------------------------------------------------------


    import pandas as pd  
    from sklearn.model_selection import train_test_split  
    from sklearn.ensemble import RandomForestClassifier  
    from sklearn.metrics import accuracy_score, classification_report, confusion_matrix  
    
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

    # Evaluate the model on the training set 
    y_train_pred = rf_model.predict(X_train) 
    accuracy_train = accuracy_score(y_train, y_train_pred) 
    conf_matrix_train = confusion_matrix(y_train, y_train_pred) 
    classification_rep_train = classification_report(y_train, y_train_pred) 

    # Evaluate the model on the test set  
    y_test_pred = rf_model.predict(X_test)  
    accuracy_test = accuracy_score(y_test, y_test_pred)  
    conf_matrix_test = confusion_matrix(y_test, y_test_pred)  
    classification_rep_test = classification_report(y_test, y_test_pred)  

    print("Training Set Metrics:") 
    print(f"Accuracy: {accuracy_train:.2f}")  
    print("\nConfusion Matrix:")  
    print(conf_matrix_train)  
    print("\nClassification Report:")  
    print(classification_rep_train)  
    
    print("\nTest Set Metrics:") 
    print(f"Accuracy: {accuracy_test:.2f}")  
    print("\nConfusion Matrix:")  
    print(conf_matrix_test)  
    print("\nClassification Report:")  
    print(classification_rep_test)  
    print("Recall: the percentage of predicted positive among all true positive") 















