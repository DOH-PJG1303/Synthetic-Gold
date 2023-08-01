# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Create LongSyntheticGold
# MAGIC
# MAGIC The purpose of this document is to provide a space where we can make sure that the email and phone processes run smoothly.
# MAGIC
# MAGIC -------------
# MAGIC
# MAGIC Author(s): PJ Gibson
# MAGIC
# MAGIC Create Date: 2022-05-03
# MAGIC
# MAGIC Last Updated Date: 2022-05-10
# MAGIC
# MAGIC Cluster: RecordLinkage Cluster

# COMMAND ----------

# MAGIC %run "../Functions/New Process Functions"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Read in data

# COMMAND ----------

# Read in the version control file that will dictacte our read in versions
versionControl = spark.read.format('delta').load(synthetic_gold_stateManagement)\
                      .withColumn('row_number', F.row_number().over(Window.partitionBy('upcoming_year').orderBy(F.desc('state_id'))))\
                      .filter('row_number == 1')

# COMMAND ----------

# For each year...
for upcoming_year in np.arange(1921,2023):
  
  # Define the version we'll read in for the current year
  my_version = versionControl.filter(f'upcoming_year == {upcoming_year}').select('SyntheticGold').collect()[0][0]
  
  # Read in the synthetic gold dataset for the desired version
  spdf_syntheticGoldCurVersion = spark.read.format('delta').option('versionAsOf',int(my_version)).load(f'{synthetic_gold_data_path}/SyntheticGold/Delta')
  
  # Create temp view
  spdf_syntheticGoldCurVersion.createOrReplaceTempView('curYearSynthGold')
  
  # If it's the first year...
  if upcoming_year == 1921:
    
    # Create the delta table with a write/overwrite command
    spdf_syntheticGoldCurVersion.write.format('delta').mode('overwrite').save(f'{synthetic_gold_data_path}/LongSyntheticGold/Delta')
    
    # Print output!
    print(f'Year {int(upcoming_year) - 1} complete!')
  
  # If we're not in the first year...
  else:
    
    # Insert into the proper table!
    spark.sql(f'''
    
    INSERT INTO delta.`{synthetic_gold_data_path}/LongSyntheticGold/Delta`
    TABLE curYearSynthGold
    
    ''')
    
    # Print output!
    print(f'Year {int(upcoming_year) - 1} complete!')

# COMMAND ----------


