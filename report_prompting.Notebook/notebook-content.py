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

# CELL ********************

import pandas as pd
import os

# CELL ********************

# read all files
basepath = "/lakehouse/default/Files"
files = os.listdir(basepath)
print(files)

# CELL ********************

summary_files = [file_name for file_name in files if file_name[-11:] == 'summary.csv']
raw_summary_files = [file_name for file_name in summary_files if file_name[-16:] != 'data_summary.csv']
groups_dict_list = []
for raw_file in raw_summary_files:
    group_dict = {"dataset_name": raw_file[:-12], 
                'raw_summary': raw_file, 
                'sinthetic_summary': raw_file[:-12] + '_synthetic_data_summary.csv'}
    groups_dict_list.append(group_dict)
print(groups_dict_list)

# CELL ********************

def create_prompt(data_dict):
    dataset_name = data_dict['dataset_name']
    raw_summary = data_dict['raw_summary']
    sinthetic_summary = data_dict['sinthetic_summary']

    prompt_1 = 'Bellow are descriptive statistics for one or more tables. \
        For each table the descriptive statistics were saved in csv format and added bellow. If a NAN value is present for a metric, \
        then the metric is not available for that specific column or no data is present. \n'

    prompt_2 = ''
    with open(os.path.join(basepath, raw_summary)) as f:
        contents = f.read()
        prompt_2 += f'--- {raw_summary} ---\n'
        prompt_2 += contents
        prompt_2 += f'------\n'
    with open(os.path.join(basepath, sinthetic_summary)) as f:
        contents = f.read()
        prompt_2 += f'--- {sinthetic_summary} ---\n'
        prompt_2 += contents
        prompt_2 += f'------\n'

    prompt_3 = 'For each descriptive statistics csv above create a statistics report as a text using values provided. Do not generate other text, only the report.\n'

    result = prompt_1 + prompt_2 + prompt_3
    return {'dataset_name': dataset_name, 'prompt': result}

# CELL ********************

return_result = []
for data_dict in groups_dict_list:
    result_dict = create_prompt(data_dict)
    return_result.append(result_dict)


# CELL ********************

mssparkutils.notebook.exit(return_result)
