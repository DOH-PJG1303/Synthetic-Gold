# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Proba Birth By Year By Age
# MAGIC 
# MAGIC The purpose of this script is to filter down one of our 
# MAGIC 
# MAGIC 
# MAGIC ----------------------
# MAGIC 
# MAGIC <p>Author: PJ Gibson</p>
# MAGIC <p>Date: 2023-01-18</p>
# MAGIC <p>Contact: peter.gibson@doh.wa.gov</p>
# MAGIC <p>Other Contact: pjgibson25@gmail.com</p>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 0. Import Libraries, Functions

# COMMAND ----------

# MAGIC %run "../../Python Global Variables"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 1. Read, Filter, Save

# COMMAND ----------

spark.read.format('csv')\
          .option('header',True)\
          .option('inferSchema',True)\
          .load(f'{synthetic_gold_supporting}/Births/03_Complete/DesiredBirthsByYearByAgeByState.csv')\
     .filter(f'State == "{stateExtended}"')\
     .drop('State')\
     .write.format('csv')\
           .mode('overwrite')\
           .option('header',True)\
           .save(f'{synthetic_gold_supporting}/Births/03_Complete/DesiredBirthsByYearByAge.csv')

# COMMAND ----------


