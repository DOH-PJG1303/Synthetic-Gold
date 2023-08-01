# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Population Table - Grandparent Generation
# MAGIC
# MAGIC The purpose of this notebook is to take the grandparent generation birthing table and add additional population info to it, then insert into the Population table.
# MAGIC
# MAGIC ## Important!!!
# MAGIC -------------------------
# MAGIC
# MAGIC <b>Ensure that you have already accomplished the following before running the script.</b>
# MAGIC <p></p>
# MAGIC
# MAGIC 1. Navigate to the [Geocorr website](https://mcdc.missouri.edu/applications/geocorr2022.html)
# MAGIC 2. Select State you wish to link to (ex: Washington) 
# MAGIC 3. Select at least 1 source geography 
# MAGIC    * 2020 Geographies > ZIP/ZCTA 
# MAGIC 4. Select at least 1 target geography.  CTRL+click for multiple 
# MAGIC    * 2020 Geographies > County 
# MAGIC 5. Select weighting variable of <b>"Population (2020 Census)"</b>
# MAGIC 5. Generate output 
# MAGIC 6. Click on .csv link to download
# MAGIC 7. Save/move output as: 
# MAGIC    * "{root_directory}/SupportingDocs/Population/01_Raw/GeoCorr_mapping<b>_zip2county</b>.csv"
# MAGIC    
# MAGIC -------------
# MAGIC
# MAGIC Author(s): PJ Gibson
# MAGIC
# MAGIC Create Date: 2022-04-05
# MAGIC
# MAGIC Last Updated Date: 2022-04-05

# COMMAND ----------

# MAGIC %run "../../Python Global Variables"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 1. Read Data

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 1.1 Birth Data

# COMMAND ----------

spdf = spark.read.format('delta').load(f'{synthetic_gold_exchange}/Births/Delta')
spdf.createOrReplaceTempView('spdf')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 1.2 GeoCorr Data
# MAGIC
# MAGIC We want to see the likelyhood that any person has a specific zip/county combination.
# MAGIC In order to do this, we use the GeoCorr data based on 2022 population counds for these two categories.
# MAGIC We will assume that the likelyhood of having a specific zip/county combination is the same in 1920 (initialization year) as it was/is in 2022.
# MAGIC Big assumption, but the best we can do for now.

# COMMAND ----------

# Read in our data, filtering to non header column (removes 1 row), format columns properly
spdf_GeoCorr = spark.read.format('csv')\
                         .option('inferSchema',True)\
                         .option('header',True)\
                         .load(f'{synthetic_gold_supporting}/Population/01_Raw/GeoCorr_mapping_zip2county.csv')\
                    .filter('county != "County code"')\
                    .filter('zcta != " "')\
                    .withColumn('pop20', F.col('pop20').astype(IntegerType()))\
                    .withColumn('zip', F.lpad('zcta', 5, '0'))\
                    .withColumn('fips', F.lpad('county', 5, '0'))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### 1.2.1 Find Probability of Zip/County cobination

# COMMAND ----------

# Calculate the total population sum over all geography combos
sum_GeoCorr_population = spdf_GeoCorr.agg(F.sum("pop20")).collect()[0][0]

# Find out those probabilities and assign a row_id
spdf_GeoCorr = spdf_GeoCorr.withColumn('perc', F.col('pop20') / sum_GeoCorr_population )\
                           .withColumn('row_id', F.row_number().over(Window.partitionBy(F.lit(1)).orderBy(F.rand(1))))

# Convert to pandas
df_GeoCorr = spdf_GeoCorr.select('row_id','perc')\
                         .toPandas()
                   
# Using our probabilities and a random choice function, get a list of the row_ids cooresponding with likely/unlikely zip & county combinations
chosen_ids = np.random.choice(df_GeoCorr['row_id'], spdf.count(), p = df_GeoCorr['perc'] )

# Convert to pyspark dataframe with 1 column
spdf_chosen_ids = pd.DataFrame(list(chosen_ids), columns=['row_id']).to_spark()

# Join back up with zip/county information
spdf_chosen_geos = spdf_chosen_ids.join(spdf_GeoCorr, on='row_id', how='left')\
                                  .select('zip','fips')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 2. Wrangle Data

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 2.1 Assign Zip & FIPS
# MAGIC
# MAGIC Using the information we just got, we'll assign a linking_id to join on and just tack on the zip/fips information onto the birthing data

# COMMAND ----------

# Tack on the linking_id to the chosen geographies
spdf_chosen_geos = spdf_chosen_geos.withColumn('linking_id', F.row_number().over(Window.partitionBy(F.lit(1)).orderBy(F.rand(2))))

# Tack on the linking_id to the birthing data
spdf = spdf.withColumn('linking_id', F.row_number().over(Window.partitionBy(F.lit(1)).orderBy(F.rand(3))))

# Join the two together. Note left join same as inner given the 1:1 matching on row_ids
spdf_new = spdf.join(spdf_chosen_geos, on='linking_id', how='left')                

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 2.2 Assign ever_partnered, house_id

# COMMAND ----------

spdf_new = spdf_new.withColumn('ever_partnered', F.lit(False))\
                   .withColumn('house_id', F.lit(-1))\
                   .withColumn('house_id', F.col('house_id').cast(LongType()))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Initialize age
# MAGIC
# MAGIC We'll go back to 1890.

# COMMAND ----------

@udf(IntegerType(),True)
def sp_get_age( s ):
  '''
  Returns a value for an individual's age given dob
  NOTE: using reference year of 1890
  
  Args:
    s (pyspark Column): dob column.
  
  Returns:
    pyspark column: giving age
  '''
  
  return int( 1890 - int(s[:4]))

# COMMAND ----------

# Get age
spdf_new = spdf_new.withColumn('age', sp_get_age(F.col('dob')))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # 3. Save

# COMMAND ----------

used_cols = ['ssn','first_name','middle_name','last_name','dob','age','sex_at_birth','race','zip','fips','house_id','ever_partnered']

spdf_new.select(used_cols).write.format('delta')\
                          .mode('overwrite')\
                          .option('overwriteSchema',True)\
                          .save(f'{root_synthetic_gold}/Data/UNFINISHED/population_pre1920/Delta')

# COMMAND ----------

spdf_new.select(used_cols)\
        .write.format('delta')\
        .mode('overwrite')\
        .option('overwriteSchema',True)\
        .save(f'{synthetic_gold_exchange}/Population/Delta')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Update SSN

# COMMAND ----------

fpath_population = f'{synthetic_gold_exchange}/Population/Delta'
fpath_ssnpool = f'{synthetic_gold_exchange}/SSNPool/Delta'


# COMMAND ----------

spark.sql(f'''

UPDATE delta.`{fpath_ssnpool}`
SET is_used = False

''').display()

# COMMAND ----------

spark.sql(f'''

MERGE INTO delta.`{fpath_ssnpool}` t
USING delta.`{fpath_population}` s
ON t.ssn = s.ssn
WHEN MATCHED
  THEN UPDATE SET t.is_used = True

''').display()

# COMMAND ----------


