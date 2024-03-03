# Synapse Analytics notebook source

# METADATA ********************

# META {
# META   "synapse": {
# META     "lakehouse": {
# META       "default_lakehouse": "e0ad8bbb-7fef-4aea-8974-29afb80ca048",
# META       "default_lakehouse_name": "lakehouse1",
# META       "default_lakehouse_workspace_id": "2ee099f6-da9f-4900-94cf-ffa73f8cbaa1"
# META     },
# META     "environment": {
# META       "environmentId": "3555fd2b-975d-4c87-bb07-a6fa3c8ac2eb",
# META       "workspaceId": "2ee099f6-da9f-4900-94cf-ffa73f8cbaa1"
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## *Data Analysis of the synthetic dataset*

# MARKDOWN ********************

# #### This notebook process the synthetic dataset following the next steps:
# - Import the necessary libraries
# - Select all the tables that we have in the lakehouse that ends with *synthetic_data.csv*
# - Analyze the synthetic datasets taking into account the type of the data for each column (for most data type we have a different summary)
# - For each table concatenate all of the results from the previous step and and save it into a csv (that contains the name of the original table in it's name )
# 
# This code can analyze for now the data in alphanumeric format. It cannot be used for images or audio data where other analysis are necessary.

# CELL ********************

import os
from pyspark.sql.types import IntegerType, DoubleType, TimestampType, StringType,BooleanType
import pyspark.sql.functions as f
from pyspark.sql.functions import col
import pyspark.pandas as ps
import pandas as pd

# CELL ********************

basepath = "/lakehouse/default/Files"
files = os.listdir(basepath)

# CELL ********************

synthetic_datasets = []
for file_name in files:
    if file_name.endswith("synthetic_data.csv"):
        synthetic_datasets.append(file_name)

# CELL ********************

def describe_numeric_data(df):
    statistics = []
    for col in df.columns:
        col_type = df.schema[col].dataType
        if isinstance(col_type, (IntegerType, DoubleType)):
            mean = df.agg({col: 'mean'}).collect()[0][0]
            std_dev = df.agg({col: 'stddev'}).collect()[0][0]
            min_val = df.agg({col: 'min'}).collect()[0][0]
            max_val = df.agg({col: 'max'}).collect()[0][0]
            quantiles = df.approxQuantile(col, [0.25, 0.5, 0.75], 0)
            skewness = df.agg({col: 'skewness'}).collect()[0][0]
            kurtosis = df.agg({col: 'kurtosis'}).collect()[0][0]
            na_count = df.filter(df[col].isNull()).count()
            total_instances = df.count()  
        
            statistics.append({
                'Column': col,  
                'Mean': mean,
                'Standard Deviation': std_dev,
                'Minimum value': min_val,
                'Maximum value': max_val,
                'Skewness': skewness,
                'Kurtosis': kurtosis,
                'NA Count': na_count,
                'Quantiles': quantiles,
                'Total Instances': total_instances
            })
    return statistics


def describe_string_data(df):
    statistics = []
    for col in df.columns:
        col_type = df.schema[col].dataType
        if isinstance(col_type, StringType):
            na_count = df.filter(df[col].isNull() | (f.trim(df[col]) == "")).count()
            non_na_df = df.filter(df[col].isNotNull() & (f.trim(df[col]) != ""))
            unique_values_count = non_na_df.select(col).distinct().count()
            mode_count = non_na_df.groupBy(col).count().orderBy("count", ascending=False).first()["count"]
            mode_value = non_na_df.groupBy(col).count().orderBy("count", ascending=False).first()[col]
            total_instances = df.count()
            
            statistics.append({
                'Column': col,
                'Unique Values Count': unique_values_count,
                'Mode': mode_value,
                'Mode Count': mode_count,
                'NA Count': na_count,
                'Total Instances': total_instances
            })
    return statistics


def describe_datetime_data(df):
    statistics = []
    for col_name in df.columns:
        col_type = df.schema[col_name].dataType
        if isinstance(col_type, TimestampType):
            na_count = df.filter(col(col_name).isNull()).count()
            min_datetime = df.agg({col_name: 'min'}).collect()[0][0]
            max_datetime = df.agg({col_name: 'max'}).collect()[0][0]
            total_instances = df.count()
            
            statistics.append({
                'Column': col_name,
                'Minimum value': min_datetime,
                'Maximum value': max_datetime,
                'NA Count': na_count,
                'Total Instances': total_instances
            })

    return statistics

def describe_boolean_data(df):
    statistics = []
    for col_name in df.columns:
        col_type = df.schema[col_name].dataType
        if isinstance(col_type, BooleanType):
            na_count = df.filter(col(col_name).isNull()).count()
            
    
            non_na_df = df.filter(col(col_name).isNotNull())
            true_count = non_na_df.filter(col(col_name) == True).count()
            false_count = non_na_df.filter(col(col_name) == False).count()
            total_instances = df.count()
            
            statistics.append({
                'Column': col_name,
                'True Count': true_count,
                'False Count': false_count,
                'NA Count': na_count,
                'Total Instances': total_instances               
            })
    
    return statistics

# CELL ********************

def concatenate_statistics(descriptive_numeric_data, descriptive_string_data,descriptive_datetime_data, descriptive_boolean_data):
    df1 = pd.DataFrame.from_dict(descriptive_numeric_data)
    df2 = pd.DataFrame.from_dict(descriptive_string_data)
    df3 = pd.DataFrame.from_dict(descriptive_datetime_data)
    df4 = pd.DataFrame.from_dict(descriptive_boolean_data)
    df_concat = pd.concat([df1, df2, df3, df4])
    return df_concat

# CELL ********************

def process_and_analyze(table_name):
    synthetic_df_path = os.path.join('Files', table_name)
    df = spark.read.format("csv").options(header=True, index_col=1).option('index_col', 0).load(synthetic_df_path)
    print(df.show(5))
    descriptive_numeric_data = describe_numeric_data(df)
    descriptive_string_data = describe_string_data(df)
    descriptive_datetime_data = describe_datetime_data(df)
    descriptive_boolean_data = describe_boolean_data(df)
    new_df = concatenate_statistics(descriptive_numeric_data, descriptive_string_data, descriptive_datetime_data, descriptive_boolean_data)
    df_path = os.path.join('Files', table_name.split('.')[0] + '_summary.csv')
    new_df.to_csv("/lakehouse/default/" + df_path)
    print(new_df.head())

# CELL ********************

for synthetic_dataset in synthetic_datasets:
    process_and_analyze(synthetic_dataset)
