# Synapse Analytics notebook source

# METADATA ********************

# META {
# META   "synapse": {
# META     "lakehouse": {
# META       "default_lakehouse": "e0ad8bbb-7fef-4aea-8974-29afb80ca048",
# META       "default_lakehouse_name": "lakehouse1",
# META       "default_lakehouse_workspace_id": "2ee099f6-da9f-4900-94cf-ffa73f8cbaa1"
# META     }
# META   }
# META }

# MARKDOWN ********************

# **NOTEBOOK SHORT SUMMARY**
# 
# This notebook generates synthetic data using a Conditional Tabular Generative Adversarial Network (CTGAN) for each dataset contained in the lakehouse  _'Tables' _ section. It trains a custom model for each data file, and stores the resulted data in a csv file, along with a few report graphics picturing real vs synthetic data distributions. 

# MARKDOWN ********************

# ##### Imports

# CELL ********************

!pip install table_evaluator
!pip install ctgan
!pip install jupyter_capture_output

# CELL ********************

import pandas as pd
import warnings
from ctgan import CTGAN
from table_evaluator import load_data, TableEvaluator
import jupyter_capture_output
import json
import PIL
from base64 import b64decode
from io import BytesIO
import IPython
import os

warnings.filterwarnings('ignore')

# MARKDOWN ********************

# ##### Capture pipeline parameters and process them further

# PARAMETERS CELL ********************

model_details_dict = ''
synthetic_dataset_len = 0

# CELL ********************

model_details_dict = model_details_dict.replace("'", '"')
model_details_dict = json.loads(model_details_dict)

# CELL ********************

SYNTHETIC_DATA_SIZE = synthetic_dataset_len

# MARKDOWN ********************

# ##### Helpers

# CELL ********************

def train_model(real_data, batch_size, epochs):
    descrete_columns = get_categorical_and_binary_columns(real_data)
    print(descrete_columns)
    model = CTGAN(batch_size = batch_size, epochs=epochs, verbose=True)
    model.fit(real_data, descrete_columns)
    return model

# CELL ********************

def generate_and_save_synthetic_data(model, size, csv_synthetic_data_path):
    synthetic_data = model.sample(size)
    synthetic_data.to_csv(csv_synthetic_data_path, index = False)

    return synthetic_data

# CELL ********************

def generate_similarity_report(synthetic_data, real_data):
    table_evaluator = TableEvaluator(real_data, synthetic_data)
    with IPython.utils.io.capture_output() as text_cap:
        table_evaluator.evaluate(target_col=real_data.columns[-1])
    with IPython.utils.io.capture_output() as img_cap:
        table_evaluator.visual_evaluation()

    return text_cap, img_cap

# CELL ********************

def save_images(images_output_capture, report_folder_path):
    index = 1
    for output in images_output_capture.outputs:
        data = output.data
        if 'image/png' in data:
            display(output)
            png_bytes = data['image/png']
            if isinstance(png_bytes, str):
                png_bytes = b64decode(png_bytes)
            bytes_io = BytesIO(png_bytes)
            img = PIL.Image.open(bytes_io)
            img.save(report_folder_path +'/figure' + str(index) + '.png', 'png')
            index += 1

# CELL ********************

def save_text(text_output_capture, report_folder_path):
    f = open(report_folder_path + "/report.txt", "w")
    f.write(text_cap.stdout)
    f.close()

# CELL ********************

def get_categorical_and_binary_columns(df):
    categorical_columns = [col for col in df.columns if df[col].dtype in ['object', 'datetime64[ns]'] ] 
    binary_columns = [col for col in df.columns if len(df[col].unique()) == 2]  
    return categorical_columns + binary_columns

# MARKDOWN ********************

# ##### Synthetic data generation and report folder generation

# CELL ********************

lakehouse_name = 'lakehouse1'
df = spark.sql("show tables in {}".format(lakehouse_name))
tables = df.select("tableName").rdd.flatMap(list).collect()

for table_name in tables:
    BATCH_SIZE, EPOCHS = model_details_dict[table_name]
    BATCH_SIZE = int(round(BATCH_SIZE, -1)) #making sure it is multiple of 10
    
    spark_data = spark.sql(f'SELECT * FROM {lakehouse_name}.{table_name}')
    synthetic_data_csv_path = '/lakehouse/default/Files/' + table_name + '_synthetic_data.csv'
    report_folder_path = '/lakehouse/default/Files/Similarity_report_' + table_name

    if not os.path.exists(report_folder_path):
        os.mkdir(report_folder_path)

    spark.conf.set("spark.sql.execution.arrow.enabled", "true")
    real_data = spark_data.toPandas()

    CTGAN_model = train_model(real_data, BATCH_SIZE, EPOCHS)

    synthetic_data = generate_and_save_synthetic_data(CTGAN_model, SYNTHETIC_DATA_SIZE, synthetic_data_csv_path)
    
    text_cap, img_cap = generate_similarity_report(synthetic_data, real_data)
    save_images(img_cap, report_folder_path)
    save_text(text_cap, report_folder_path)



