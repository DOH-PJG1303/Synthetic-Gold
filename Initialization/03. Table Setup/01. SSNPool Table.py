# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # SSNPool Table
# MAGIC
# MAGIC The purpose of this script is to create a 1-column table with 30 million generated SSN numbers.
# MAGIC The SSNPool table is a 1 column table that will be dwindled as the process runs and SSN values are taken.
# MAGIC We do not really intend this field to be used heavily for record linkage, so it's a bit overkill to make it as accurate as we can.
# MAGIC The main purpose of this is to have unique values for each person.
# MAGIC
# MAGIC ### History of SSN
# MAGIC
# MAGIC The following information was derived from the following location - [SSN history](https://www.ssa.gov/history/ssn/geocard.html)
# MAGIC
# MAGIC --------
# MAGIC
# MAGIC Prior to 1972, SSN values are composed of three parts : <b>AAA-GG-SSSS</b>
# MAGIC * AAA - Area number, this value was state-specific. These numbers for all states were taken from the [following source](https://www.uclaisap.org/trackingmanual/manual/appendix-G.html)
# MAGIC * GG - Group number, see quote below. Taken from article hyperlinked above:
# MAGIC > Within each area, the group number (middle two (2) digits) range from 01 to 99 but are not assigned in consecutive order. For administrative reasons, group numbers issued first consist of the ODD numbers from 01 through 09 and then EVEN numbers from 10 through 98, within each area number allocated to a State. After all numbers in group 98 of a particular area have been issued, the EVEN Groups 02 through 08 are used, followed by ODD Groups 11 through 99.
# MAGIC * SSSS - Security number, incrimental IDs ranging from 0000 - 9999.  
# MAGIC
# MAGIC Note that in 2011, SSN assignment became less predictable and more randomized.  
# MAGIC There is still a procedure though, just extremely complex.
# MAGIC
# MAGIC Other fun facts, SSN values were not assigned at birth until 1984. 
# MAGIC And SSN values were created in 1936 to track workers' lifetime earnings.
# MAGIC
# MAGIC -------
# MAGIC
# MAGIC ### Goal: Create a 1-column table with a 10% surplus-pad of generated SSN numbers.
# MAGIC
# MAGIC The SSNPool table is a 1 column table that will be dwindled as the process runs and SSN values are taken.
# MAGIC The main purpose of assigning SSN values is to have unique values for each person.
# MAGIC
# MAGIC
# MAGIC ----------------------
# MAGIC
# MAGIC
# MAGIC <p>Author: PJ Gibson</p>
# MAGIC <p>Date: 2021-12-17</p>
# MAGIC <p>Contact: peter.gibson@doh.wa.gov</p>
# MAGIC <p>Other Contact: pjgibson25@gmail.com</p>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 0. Load in Python Global Variables

# COMMAND ----------

# DBTITLE 0,Load libraries/paths
# MAGIC %run "../../Python Global Variables"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 0.1 Calculate Desired SSN Count by time period
# MAGIC
# MAGIC
# MAGIC Find out number of SSNs to generate pre2011 and post2011
# MAGIC Add 10% for a nice cusion.
# MAGIC
# MAGIC ```python
# MAGIC multiplier = 1.1
# MAGIC Pre2011 = (Population 1920 + sum(births 1920-2011)) * multiplier
# MAGIC Post2011 = (sum(births 2011-2025)) * multiplier
# MAGIC ```

# COMMAND ----------

# Read in our vital stats record, filtering to our state
vitalStats = spark.read.format('csv')\
                  .option('inferSchema',True)\
                  .option('header',True)\
                  .load(f'{synthetic_gold_supporting}/Births/03_Complete/VitalStats_byYear_byState.csv')\
                  .filter(f'State == "{stateExtended}"')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### 0.1.1 Desired SSNs pre-2011

# COMMAND ----------

# Find number of SSNs we initialize right off the start of our simulation
population1920 = vitalStats.filter('Year == 1920')\
                           .select('Population').collect()[0][0]

# Find number of SSNs assigned post initialization & pre-2011
sum_births_1920_2011 = vitalStats.filter('(Year >= 1920) AND (Year < 2011)')\
                                 .agg(F.sum('Births')).collect()[0][0]

# Add our previous 2 variables and give a padding of 10% to create a surplus of SSNs in our SSNPool
desired_ssns_pre2011 = (population1920 + sum_births_1920_2011) * 1.1

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### 0.1.2 Desired SSNs post-2011

# COMMAND ----------

# Find number of SSNs assigned post 2011
sum_births_post_2011 = vitalStats.filter('(Year >= 1911)')\
                                 .agg(F.sum('Births')).collect()[0][0]

# Give a padding of 10% to create a surplus of SSNs in our SSNPool
desired_ssns_post2011 = sum_births_post_2011 * 1.1

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 1. SSN Values:  pre-2011
# MAGIC
# MAGIC As mentioned above, prior to 2011, there was a pretty standardized method for assigning SSN values.
# MAGIC Below, we attempt to replicate this method so that available SSN values can be pulled in a specific order prior to the 2011 iteration of our process.
# MAGIC
# MAGIC We should note that while SSN values were not assigned at birth until the late 1900s, our process will assign them at birth for all year-iterations.  
# MAGIC This makes the most sense because the SSN field will serve as the primary key for most table connections.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 1.1 AAA section (first 3)

# COMMAND ----------

# Read in state specific data from supporting documents
AAA = spark.read.format('csv')\
           .option('inferSchema',True)\
           .option('header',True)\
           .load(f'{synthetic_gold_supporting}/SSN/03_Complete/ssnF3_crosswalk_final.csv')\
           .filter(f'State == "{stateExtended}"')\
           .withColumnRenamed('Values','area')\
           .withColumn('area', F.lpad(F.col('area'), 3, '0'))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 1.2 BB section (middle 2)

# COMMAND ----------

# Create spark dataframes, making sure to note which section they fall in
GG1 = pd.DataFrame(pd.Series(np.arange(1,10,2), name='group')).to_spark().withColumn('Preorder', F.lit(1))
GG2 = pd.DataFrame(pd.Series(np.arange(10,100,2), name='group')).to_spark().withColumn('Preorder', F.lit(2))
GG3 = pd.DataFrame(pd.Series(np.arange(2,9,2), name='group')).to_spark().withColumn('Preorder', F.lit(3))
GG4 = pd.DataFrame(pd.Series(np.arange(11,100,2), name='group')).to_spark().withColumn('Preorder', F.lit(4))

# Union 4 parts, create a GGRowNumber to track the order properly, apply lpad to add 0
GG = GG1.unionByName(GG2).unionByName(GG3).unionByName(GG4)\
        .withColumn('GGRowNumber', F.row_number().over(Window.partitionBy(F.lit(1)).orderBy(F.col('Preorder'), F.col('group'))))\
        .drop('PreOrder')\
        .withColumn('group', F.lpad(F.col('group'), 2, '0'))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 1.3 SSSS section (last 4)

# COMMAND ----------

SSSS = pd.DataFrame(pd.Series(np.arange(0,10000), name='ssss')).to_spark()\
         .withColumn('ssss', F.lpad(F.col('ssss'),4,'0'))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 1.4 Create SSNs

# COMMAND ----------

spdf_pre2011 = AAA.join(GG, how='cross')\
                   .join(SSSS, how='cross')\
                   .withColumn('process_end_date', F.lit(2011))\
                   .withColumn('ssn', F.concat_ws('-', 'area', 'group', 'ssss'))\
                   .withColumn('index', F.row_number().over(Window.partitionBy(F.lit(1)).orderBy('area','GGRowNumber','ssss')))\
                   .withColumn('is_used', F.lit(False))\
                   .select(['index','area','ssn','process_end_date','is_used'])\
                   .filter(f'index <= {int(desired_ssns_pre2011)}') # Filter our records down to reduce volume

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 2. SSN Values:  post-2011
# MAGIC
# MAGIC As mentioned in the initial markdown, after 2011 the SSN assignment process became significantly more complex.
# MAGIC I believe this was in an attempt to make the assignment more randomized.  
# MAGIC
# MAGIC Because of this, we will create a simplified representation of this by assigning SSN values completely randomly.
# MAGIC We do this by disregarding Washington-specific SSN area number, and randomizing group number / serial number.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 2.1 Find new AAA values
# MAGIC
# MAGIC Get all numbers between 001-999.
# MAGIC Will eventually anti-join to make sure they don't overalp with other AAA values for uniqueness-sake

# COMMAND ----------

# Create dataframe from 001-999 inclusive, ensuring to pad the lower digits with 0
AAA2 = pd.DataFrame(pd.Series(np.arange(1,1000), name='area')).to_spark()\
         .withColumn('area', F.lpad(F.col('area'), 3, '0'))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 2.2 Create SSNs

# COMMAND ----------

spdf_post2011 = AAA2.join(GG, how='cross')\
                    .join(SSSS, how='cross')\
                    .withColumn('process_end_date', F.lit(2035))\
                    .withColumn('ssn', F.concat_ws('-', 'area', 'group', 'ssss'))\
                    .join(spdf_pre2011.select('ssn'), on='ssn', how='left_anti')\
                    .withColumn('index', F.row_number().over(Window.partitionBy(F.lit(1)).orderBy(F.rand(1))))\
                    .withColumn('is_used', F.lit(False))\
                    .select(['index','area','ssn','process_end_date','is_used'])\
                    .filter(f'index <= {int(desired_ssns_post2011)}')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 3. Join & Save

# COMMAND ----------

SSNPool = spdf_pre2011.unionByName(spdf_post2011)

# COMMAND ----------

# Save to spark dataframe
SSNPool.write.format('delta')\
       .mode('overwrite')\
       .option('overwriteSchema',True)\
       .save(f'{synthetic_gold_exchange}/SSNPool/Delta')

# COMMAND ----------


