# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # DEFINE YOUR STATE

# COMMAND ----------

state = 'WA'
stateExtended = 'Washington'
stateFIPS = '53'

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Python Global Variables
# MAGIC
# MAGIC The purpose of this document is to have one location for all base-directories for working projects.  
# MAGIC
# MAGIC * <b>Paths</b>
# MAGIC   * path names: lowercase, snake_case
# MAGIC   * all end WITHOUT a '/'
# MAGIC   * project level directories labeled as "root_{projectname}"
# MAGIC
# MAGIC * <b>Variables</b>
# MAGIC   * lowercase, snake_case
# MAGIC   * widely applicable
# MAGIC   * no non-native library usage (want to run from any cluster)
# MAGIC   
# MAGIC   
# MAGIC \* Google docstring format shown below
# MAGIC
# MAGIC
# MAGIC See the below example of the docstring format you are expected to follow. 
# MAGIC Note that the function name is camelcase and the actual function is well-documented.
# MAGIC
# MAGIC ```python
# MAGIC
# MAGIC def MakeLowercase(df, colname, print_output = False):
# MAGIC     '''
# MAGIC     Turns a pandas dataframe column into all lowercase
# MAGIC
# MAGIC     Args:
# MAGIC         df: pandas dataframe
# MAGIC           pandas dataframe you will be operating on
# MAGIC         colname: str
# MAGIC           name of pandas dataframe column. Column must be string type
# MAGIC         *print_output: bool (default=False)
# MAGIC           if True, prints string indicating the action has taken place
# MAGIC           
# MAGIC     Returns:
# MAGIC         A dataframe where the indicated column is now transformed to lowercase
# MAGIC     '''
# MAGIC     
# MAGIC     # Make column lowercase
# MAGIC     df[colname] = df[colname].str.lower()
# MAGIC     
# MAGIC     # Return printed output if specified
# MAGIC     if print_output:
# MAGIC       print(f'Succesfully converted column "{colname}" to lowercase.')
# MAGIC       
# MAGIC     return df
# MAGIC   
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Libraries
import requests
import json
import numpy as np
import pyspark.pandas as pd
import re
import pandas

from datetime import date, datetime, timedelta
from pytz import timezone

import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql import SQLContext

from delta.tables import DeltaTable
 
# import databricks.koalas as ks

pd.options.display.max_rows = 20

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Paths

# COMMAND ----------

# DBTITLE 1,Define paths
root_synthetic_gold = f"path/to/Synthetic{stateExtended}"

synthetic_gold_data_path = f"{root_synthetic_gold}/Data"
synthetic_gold_exchange = f'{root_synthetic_gold}/Data/Exchange'
synthetic_gold_supporting = f'{root_synthetic_gold}/SupportingDocs'
synthetic_gold_stateManagement = f'{synthetic_gold_data_path}/StateManagement/Delta'


# COMMAND ----------

# DBTITLE 1,Print Paths
print(f"root_synthetic_gold: {root_synthetic_gold}")
print(f"synthetic_gold_data_path: {synthetic_gold_data_path}")
print(f"synthetic_gold_exchange: {synthetic_gold_exchange}")
print(f"synthetic_gold_supporting: {synthetic_gold_supporting}")
print(f"synthetic_gold_stateManagement: {synthetic_gold_stateManagement}")

# COMMAND ----------

# MAGIC %md ## Variables

# COMMAND ----------

# DBTITLE 0,Define variables
######################################################################################################
# Dates & Times
######################################################################################################

# For Today:
current_date = datetime.now().astimezone(timezone('US/Pacific')).strftime("%Y%m%d")
current_date_formatted = datetime.now().astimezone(timezone('US/Pacific')).strftime("%Y-%m-%d")
current_datetime = datetime.now().astimezone(timezone('US/Pacific')).strftime("%Y%m%d%H%M")
current_weekday = datetime.now().astimezone(timezone('US/Pacific')).date().strftime("%A")

# For Yesterday
yesterday_date = (datetime.now().astimezone(timezone('US/Pacific')).date() - timedelta(days = 1)).strftime("%Y%m%d")
yesterday_date_formatted = (datetime.now().astimezone(timezone('US/Pacific')).date() - timedelta(days = 1)).strftime("%Y-%m-%d")
yesterday_weekday = (datetime.now().astimezone(timezone('US/Pacific')).date() - timedelta(days = 1)).strftime("%A")

# If you want to skip weekends...
if current_weekday == 'Monday':
  last_weekday_date = (datetime.now().astimezone(timezone('US/Pacific')).date() - timedelta(days = 3)).strftime("%Y%m%d")
  last_weekday_date_formatted = (datetime.now().astimezone(timezone('US/Pacific')).date() - timedelta(days = 3)).strftime("%Y-%m-%d")
  last_weekday_weekday = (datetime.now().astimezone(timezone('US/Pacific')).date() - timedelta(days = 3)).strftime("%A")
  
else:
  last_weekday_date = yesterday_date
  last_weekday_date_formatted = yesterday_date_formatted
  last_weekday_weekday = yesterday_weekday
  
######################################################################################################
# Sythetic gold
######################################################################################################

# load information on FIPS stuff
fips_crosswalk = spark.read.format('csv')\
                      .options(inferSchema=True, header=True)\
                      .load(f'{synthetic_gold_supporting}/Housing/03_Complete/county_FIPS_info.csv')\
                      .filter(f'STATE == "{state}"')

# COMMAND ----------

# DBTITLE 1,Print Variables
print('Dates & Times')
print('-----------------')

print(f"current_date: {current_date}")
print(f"current_date_formatted: {current_date_formatted}")
print(f"current_datetime: {current_datetime}")
print(f"current_weekday: {current_weekday}")

print(f"yesterday_date: {yesterday_date}")
print(f"yesterday_date_formatted: {yesterday_date_formatted}")
print(f"yesterday_weekday: {yesterday_weekday}")

print(f"last_weekday_date: {last_weekday_date}")
print(f"last_weekday_date_formatted: {last_weekday_date_formatted}")
print(f"last_weekday_weekday: {last_weekday_weekday}")

print('\nSynthetic Gold')
print('-----------------')
print('dict_FIPS_crosswalk: Dictionary with 5 digit fips to county names.')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Functions

# COMMAND ----------

def file_exists(path):
  '''
  Checks to see if a path exists, if it does, return True

  Args:
      path: string

  Returns:
      True if path exists, False if doesn't exist
      * raises error if path invalid or user inputs something invalid into the path string.
  '''
  
  try:
    dbutils.fs.ls(path)
    return True
  except Exception as e:
    if 'java.io.FileNotFoundException' in str(e):
      return False
    else:
      raise

# COMMAND ----------

def validate_api_request( response ):
  '''
  Checks to see if 200 is in the response text, if so query was successful
  
  Args:
      response - str, value from `response.get(url)` call

  Returns:
      print('API Success!') when successful
      ValueError('API call unsuccessful.  Try checking your URL again.')
      
  '''

  if ('200' in str(response)):
    print('API Success!')
    
  else:
    raise ValueError('API call unsuccessful.  Try checking your URL again.')
  

# COMMAND ----------

def custom_normalize( numpy_list, rounded = 5 ):
  '''
  The purpose of this function is to normalize a numpy vector in a way where they all sum to 1 and have values rounded to a specific value.
  Any extra bits will be added to the first element of the list
  
  Args:
      numpy_list - numpy.array, list to be normalized
      rounded - int, integer to round values to. 
            * Note: rounded to 1 would mean output looks like [0.4, 0.3, 0.2, 0.1], meaning rounding to 10% intervals

  Returns:
      new normalized list
  '''
  
  output = list(np.round( numpy_list / numpy_list.sum() , rounded ))
  remainder = 1.0 - sum(output)
  
  if remainder == 0:
    return output
  
  else:
    output[0] += remainder
    return output
  
