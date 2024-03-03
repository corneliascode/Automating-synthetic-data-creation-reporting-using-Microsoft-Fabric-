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

import os
import pandas as pd

# PARAMETERS CELL ********************

label_pred = ''

# CELL ********************

#label = "diabetes:Y"

# CELL ********************

dataset_name = label_pred.split(':')[0]
dataset_label = label_pred.split(':')[1]
summary_path = os.path.join('/lakehouse/default/Files', dataset_name + '_summary.csv')
df = pd.read_csv(summary_path, index_col=1)
label_unique_values = df.at[dataset_label, 'Unique Values Count']

model_type = ''

if label_unique_values > 5:
    model_type = 'regression'
elif label_unique_values <= 5:
    model_type = 'classification'


output = ':'.join((dataset_name, dataset_label, model_type))

# CELL ********************

mssparkutils.notebook.exit(output)
