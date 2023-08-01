# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Compile Phone Numbers
# MAGIC 
# MAGIC The purpose of this script is to create a list of preliminary phone numbers we will use in our dataset.
# MAGIC 
# MAGIC 
# MAGIC ----------------------
# MAGIC 
# MAGIC <p>Author: PJ Gibson</p>
# MAGIC <p>Date: 2023-01-03</p>
# MAGIC <p>Contact: peter.gibson@doh.wa.gov</p>
# MAGIC <p>Other Contact: pjgibson25@gmail.com</p>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # 1. Import Libraries, Functions

# COMMAND ----------

# MAGIC %run "../../Python Global Variables"

# COMMAND ----------

# Random seed set in "../../Python Global Variables" above
rng = np.random.default_rng( 42 )

# COMMAND ----------

# Set pandas option
pd.set_option("compute.ops_on_diff_frames" , True)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # 2. Read Data

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Zip to Area Code Map
# MAGIC 
# MAGIC Note that this data was downloaded from the following website, [linked here](https://www.unitedstateszipcodes.org/zip-code-database/)
# MAGIC 
# MAGIC This data source was found online and <b>CAN NOT</b> be used for commercial use.
# MAGIC 
# MAGIC For Washington State specifically (state = "WA"):
# MAGIC 
# MAGIC * <b>7.615 million</b> - census population estimate (2019)
# MAGIC * <b>6,848,037</b> - sum of [irs_estimated_population] field within this dataset
# MAGIC * <b>6,580,148</b> = sum of [irs estimated population] field AFTER removing rows with no area code
# MAGIC 
# MAGIC -----
# MAGIC 
# MAGIC For this reason, as well as for the purposes of creating an excessive number of phone numbers in our PhoneLookup table, we'll create <b>3x the population count</b> number of phone numbers for each zip code.

# COMMAND ----------

df_mapping = pd.read_csv(f"{root_synthetic_gold}/SupportingDocs/Phone/01_Raw/USZIPCODES_map_zip_ac.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Flter to state specific

# COMMAND ----------

df_map_state = df_mapping.query(f'(state == "{state}") AND (area_codes != "None")')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # 3. Wrangle Data

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Area Codes

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Split columns with multiple area codes

# COMMAND ----------

df_map_state['list_area_codes'] = df_map_state['area_codes'].astype(str).str.split(',')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Find out number of area codes
# MAGIC 
# MAGIC Necissary for calculating the population within each zip code that applies to each area code

# COMMAND ----------

df_map_state['number_ac'] = df_map_state['list_area_codes'].str.len()

# COMMAND ----------

df_map_state['irs_estimated_population_adjusted'] = df_map_state['irs_estimated_population'] / df_map_state['number_ac']

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Explode rows that had multiple area codes

# COMMAND ----------

df_map_state_exploded = df_map_state.explode('list_area_codes')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Filter columns

# COMMAND ----------

df_zip_ac_population = df_map_state_exploded[['zip','list_area_codes','irs_estimated_population_adjusted']]
df_zip_ac_population.rename(columns={'list_area_codes':'ac', 'irs_estimated_population_adjusted':'population'}, inplace=True)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Get 3x rows we need
# MAGIC 
# MAGIC To be generous with our lookup table, we're going to have 3x the population count number of phones for each zip code
# MAGIC To do this, we'll need a custom UDF to then explode and make new columns

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Define function

# COMMAND ----------

@udf(ArrayType(IntegerType(), True))
def get_3pop_range(population):
  '''
  Returns a column containing a list of all available ints between [1, population*3]
  Intended for this new column to later be exploded
  
  Args:
    population (pyspark Column): int64 type, nullable
  
  Returns:
    pyspark column as described above
  '''
  if population is None:
    return list(np.nan)
  
  else:
    return list(range(1,int((population*3)+1)))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Impliment Function, Explode column

# COMMAND ----------

# Convert to pyspark
spdf_zip_ac_population = df_zip_ac_population.to_spark()

# Add new column using our function
spdf_zip_ac_population = spdf_zip_ac_population.withColumn('pop_range', get_3pop_range(F.col('population')))

# Explode column
spdf_zip_ac_population_exploded = spdf_zip_ac_population.withColumn('pop_exploded', F.explode(F.col('pop_range')))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Assign row ID
# MAGIC 
# MAGIC Dependent on area code.
# MAGIC Ascending count from 1 - 3x population within AC.

# COMMAND ----------

#  Assign row_id.  Winodow funciton row number within AC group, random order
spdf_zip_ac_population_exploded = spdf_zip_ac_population_exploded.withColumn("ac_row_id",F.row_number().over(Window.partitionBy( 'ac' ).orderBy( F.rand(42) )))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Phone Numbers

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Initialize last 7 digit options

# COMMAND ----------

# Get dataframe of all available values for phone number last 7 digits
df_phone_nubmers = pd.DataFrame(np.arange(2*10**6,10**7),columns=['phone_suffix'])

# Convert to pyspark
spdf_phone_numbers = df_phone_nubmers.to_spark()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Explode data for each Area Code

# COMMAND ----------

# Identify unique area codes we'll compile phone numbers for
list_acs = spdf_zip_ac_population_exploded.groupBy('ac').count().select('ac').rdd.flatMap(lambda x: x).collect()

# Create column indicating the list of acceptable ZIP codes
spdf_phone_numbers = spdf_phone_numbers.withColumn('list_ACs' ,  F.array([F.lit(ac) for ac in list_acs]))

# Explode column
spdf_phone_nubmers_exploded = spdf_phone_numbers.withColumn('AC',F.explode('list_ACs'))

# Filter data
spdf_phone_nubmers_exploded = spdf_phone_nubmers_exploded.select('AC','phone_suffix')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Assign row_id

# COMMAND ----------

# Define our window using previous params
my_window = Window.partitionBy( 'AC' ).orderBy( F.rand(42) )

# Add column to DF.  Row number within AC group, random order
spdf_phone_nubmers_exploded = spdf_phone_nubmers_exploded.withColumn("ac_row_id",F.row_number().over(my_window))

# Drop AC so it doesn't conflict
spdf_phone_nubmers_exploded = spdf_phone_nubmers_exploded.withColumnRenamed('AC','AC2')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # 4. Join data

# COMMAND ----------

# Join data to get both AC and ZIP and Phone last 7 in the same dataframe
spdf_output = spdf_zip_ac_population_exploded.join(spdf_phone_nubmers_exploded, 
                                                  (spdf_zip_ac_population_exploded.ac == spdf_phone_nubmers_exploded.AC2) & (spdf_zip_ac_population_exploded.ac_row_id == spdf_phone_nubmers_exploded.ac_row_id),
                                                   how = 'inner')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # 5. Save

# COMMAND ----------

spdf_output.select('zip','ac','phone_suffix').write.format('delta').mode('overwrite').save(f"{root_synthetic_gold}/SupportingDocs/Phone/03_Complete/phone_lookup/Delta")

# COMMAND ----------


