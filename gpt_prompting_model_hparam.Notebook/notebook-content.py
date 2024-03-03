# Synapse Analytics notebook source

# METADATA ********************

# META {
# META   "synapse": {
# META     "lakehouse": {
# META       "default_lakehouse": "5dd598b3-0049-4e7a-aca4-8a71ba1ae005",
# META       "default_lakehouse_name": "casadinpadure",
# META       "default_lakehouse_workspace_id": "6b6606be-3275-4558-944e-aac247cc42a4"
# META     },
# META     "environment": {
# META       "environmentId": "3555fd2b-975d-4c87-bb07-a6fa3c8ac2eb",
# META       "workspaceId": "2ee099f6-da9f-4900-94cf-ffa73f8cbaa1"
# META     }
# META   }
# META }

# MARKDOWN ********************

# **Notebook Short Summary**
# 
# The notebook uses OpenAI in order to obtain a fit configuration for the CTGAN's hyperparameters(batch_size, epochs number), based on the real dataset size(number of variables, number of entries). It returns a list of hyperparameters values for each table from the lakehouse  _'Tables'_  section, which will be further used for training.

# MARKDOWN ********************

# ##### Imports

# CELL ********************

import os
from openai import AzureOpenAI
from notebookutils import mssparkutils
import json

# MARKDOWN ********************

# ##### OpenAI client configuration

# CELL ********************

AZURE_OPENAI_KEY = '67acbbf4-ab99-4006-ba16-495bafea4718'
AZURE_OPENAI_ENDPOINT = 'https://polite-ground-030dc3103.4.azurestaticapps.net/api/v1'

# CELL ********************

client = AzureOpenAI(
    api_key=AZURE_OPENAI_KEY,  
    api_version="2023-09-01-preview",
    azure_endpoint=AZURE_OPENAI_ENDPOINT
)

# MARKDOWN ********************

# ##### Capture pipeline parameters and process them further

# PARAMETERS CELL ********************

dict_configurations = ''

# CELL ********************

dict_configurations = dict_configurations.replace("'", '"')
dict_configurations = json.loads(dict_configurations)

# MARKDOWN ********************

# Helpers

# CELL ********************

def find_string_between(s, first, last):
     start = s.index( first ) + len( first )
     end = s.index( last, start ) 
     return s[start:end]

# MARKDOWN ********************

# ##### Hyperparameters configuration generation with OpenAI

# CELL ********************

prompts_dict = dict()
for key,value in dict_configurations.items():
    dataset_columns = value[0]
    dataset_rows = value[1]
    prompt = "Given a dataset with " +  str(dataset_columns)  + " columns and " + str(dataset_rows) + " rows, give me a sample configuration consisting of [batch_size, epochs_number] for training a CTGAN (Conditional Tabular GAN) for synthetic data generation, where batch_size is divisible with 10. Do not give me other explanations or prompts."
    prompts_dict[key] = prompt


# CELL ********************

model_details_dict = dict()
for key, prompt in prompts_dict.items():
    response = client.chat.completions.create(
        model="gpt-35-turbo",
        messages=[
            {"role": "user", "content": prompt}
        ]
    )

    #print(response)
    # print(response.model_dump_json(indent=2))
    print(response.choices[0].message.content)
    output_response = response.choices[0].message.content
    filtered_output = find_string_between(output_response, '[', ']').split(',')
    final_output = [int(x) for x in filtered_output]
    model_details_dict[key] = final_output


# MARKDOWN ********************

# Send the output further in the pipeline for the training notebook.

# CELL ********************

mssparkutils.notebook.exit(model_details_dict)
