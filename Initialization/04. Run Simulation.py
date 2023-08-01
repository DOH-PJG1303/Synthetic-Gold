# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Run V2
# MAGIC
# MAGIC
# MAGIC The purpose of this document is to provide a space where we can run the whole process version 2 in one neat little cell
# MAGIC
# MAGIC -------------
# MAGIC
# MAGIC Author(s): PJ Gibson
# MAGIC
# MAGIC Create Date: 2022-05-19
# MAGIC
# MAGIC Last Updated Date: 2022-05-19
# MAGIC
# MAGIC Cluster: RecordLinkage Cluster

# COMMAND ----------

# MAGIC %run "../Functions/New Process Functions"

# COMMAND ----------

# MAGIC %run "../Functions/State Management Functions"

# COMMAND ----------

# Should start in 1890
list_years = np.arange(1890,2023)

for i in np.arange(0,len(list_years)):
  
  year = int(list_years[i])
  
  print(f'\n{year}\n--------\n')

  # Run the simulation
  run_simulation( year )

# COMMAND ----------

# # In the case of any hiccups along the way, revert to an old version.
# # NOTE: THIS IS SOMETHING THAT YOU CANNOT UNDO
# revert_year = 1912
# revertState( synthetic_gold_stateManagement, revert_year )

# COMMAND ----------

spdf = spark.read.format('delta').load(f'{synthetic_gold_data_path}/SyntheticGold/Delta')
spdf.display()

# COMMAND ----------


