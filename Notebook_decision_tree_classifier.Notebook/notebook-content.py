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

# ##  *Decision Tree Classifier*

# MARKDOWN ********************

# #### This notebook trains & evaluate a decision tree classifier for classification tasks following the next steps:
# 
# - Loads CSV datasets using Spark and converts them to pandas DataFrames for easier manipulation.
# - Splits pandas DataFrames into training and testing sets, allowing us to evaluate model performance.
# - Trains a decision tree regression model with customizable parameters (e.g. maximum depth)
# - Calculates feature importances to understand the relative influence of each feature on the model's predictions.
# - Makes predictions on unseen data using the trained model.
# - Evaluates model performance by calculating common metrics like Accuracy, Precision, Recall, F1 Score and Confusion_matrix

# CELL ********************

import os
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeClassifier
from sklearn import metrics
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, confusion_matrix

# PARAMETERS CELL ********************

label_pred = ''

# CELL ********************

#label = 'diabetes:Y:classification'

# CELL ********************

model_data_params = {'dataset_name': label_pred.split(':')[0] + '_synthetic_data.csv', 'dataset_label': label_pred.split(':')[1]}

# CELL ********************

def load_spark_dataset(dataset_name):
    """
    Loads the dataset using Spark.

    Args:
        dataset_name (str): Name of the dataset file (including extension).

    Returns:
        pyspark.sql.DataFrame: The loaded Spark DataFrame.
    """
    df_path = os.path.join('Files', dataset_name)
    df = spark.read.format("csv").option("header", "true").load(df_path)
    pandas_df = df.toPandas()
    return pandas_df



def split_dataset(dataset, dataset_label, test_size=0.3, random_state=None):
    """
    Splits the data into training and testing sets.

    Args:
        dataset (pandas.DataFrame): The dataset.
        dataset_label (str): Name of the dependent variable column.
        test_size (float, optional): Proportion of data for testing. Defaults to 0.3.
        random_state (int, optional): Random seed for reproducibility. Defaults to None.

    Returns:
        tuple: A tuple containing training and testing data splits.
    """
    dependent_variable = dataset_label
    dependent_variable_data = dataset[dependent_variable]
    independent_variables = dataset.drop([dependent_variable], axis=1)
    
    X = independent_variables
    y = dependent_variable_data
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size, random_state=random_state)  
    return X_train, X_test, y_train, y_test



def train_decision_tree(X_train, y_train, max_depth=5):
    """
    Trains a Decision Tree regression model and calculates feature importances.

    Args:
        X_train (pandas.DataFrame): The training data features.
        y_train (pandas.Series): The training data target variable.
        max_depth (int, optional): The maximum depth of the decision tree. Defaults to 5.

    Returns:
        tuple: A tuple containing the trained model and feature importances.
    """

    tree_classifier = DecisionTreeClassifier(max_depth=max_depth)
    tree_classifier.fit(X_train, y_train)


    feature_importances = []
    for name, importance in zip(X_train.columns, tree_classifier.feature_importances_):
        feature_importances.append({'name': name, 'importance': importance})
    return tree_classifier, feature_importances



def make_predictions(model, X_test):
  """
  Makes predictions using a trained model.

  Args:
      model (object): The trained model object.
      X_test (pandas.DataFrame): The testing data features.

  Returns:
      pandas.Series: The predicted target values.
  """
  y_pred = model.predict(X_test)
  return y_pred



def evaluate_model(y_test, y_pred):
    """
    Evaluates the performance of a model and returns classification metrics as a dictionary.

    Args:
        y_test (pandas.Series): The actual target values for the testing data.
        y_pred (pandas.Series): The predicted target values.

    Returns:
        dict: A dictionary containing the calculated evaluation metrics.
    """
    accuracy = accuracy_score(y_test, y_pred)
    precision = precision_score(y_test, y_pred, average='weighted', zero_division=1)
    recall = recall_score(y_test, y_pred, average='weighted', zero_division=1)
    f1 = f1_score(y_test, y_pred, average='weighted', zero_division=1)
    confusion = confusion_matrix(y_test, y_pred)
    
    metrics = {
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall,
        "f1_score": f1,
        "confusion_matrix": confusion
    }
    return metrics

# CELL ********************

dataset_df = load_spark_dataset(model_data_params['dataset_name'])
X_train, X_test, y_train, y_test = split_dataset(dataset_df, model_data_params['dataset_label'], test_size=0.3, random_state=None)
tree_classifier, feature_importances = train_decision_tree(X_train, y_train, max_depth=5)
prediction = make_predictions(tree_classifier, X_test)
metrics = evaluate_model(y_test, prediction)
print(metrics)
print("Feature importances:", feature_importances)
output = {'metrics':metrics, 'feature_importances':feature_importances}

# CELL ********************

# save output to text
text_file_path = os.path.join('/lakehouse/default/Files', label_pred.split(':')[0] + '_synthetic_model_report.txt')
metrics = output['metrics']
feature_imp = output['feature_importances']
with open(text_file_path, 'w') as f:
    f.write('---- Decission tree classifier metrics ----\n\n')
    for each in metrics:
        f.write(f"{each}: {metrics[each]}\n")
    f.write('\n')
    f.write('---- Decission tree classifier features importance ----\n\n')
    for each in feature_imp:
        f.write(f"{each['name']}: {each['importance']}\n")

# CELL ********************

mssparkutils.notebook.exit(output)
