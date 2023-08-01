# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Create Fname Lookup
# MAGIC
# MAGIC The purpose of this script is to create a bank of Fname/Middlenames (serve the same purpose).
# MAGIC As part of the birthing process, people are assigned names.
# MAGIC It would take too much processing to load in the name probability data & subsequently generate random names.
# MAGIC
# MAGIC In the new process, we hope to:
# MAGIC 1. Create a bank of firstnames with respective race/sex at birth info
# MAGIC 2. Load it in during birthing process
# MAGIC 3. Shuffle the data randomly
# MAGIC 4. Join it to new births by race/sex/F.row_number() within race/sex window partition.
# MAGIC
# MAGIC <b>This script focuses on generating the dataset used in Step 1.</b>
# MAGIC
# MAGIC -------------
# MAGIC
# MAGIC ## Note this script can take nearly 2 hours to create
# MAGIC
# MAGIC -------------
# MAGIC
# MAGIC Author(s): PJ Gibson
# MAGIC
# MAGIC Create Date: 2022-04-25
# MAGIC
# MAGIC Last Updated Date: 2022-04-25

# COMMAND ----------

# MAGIC %run "../../Python Global Variables"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # 1. Initialize Data

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### First Name
# MAGIC
# MAGIC Raw data to support the probabilities of having a given firstname given your sex at birth is [linked here](https://www.ssa.gov/oact/babynames/limits.html)
# MAGIC
# MAGIC Raw data to support the probabilities of having a given firstname given your race is [linked here](https://doi.org/10.7910/DVN/TYJKEZ)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Middle Name
# MAGIC
# MAGIC Middle names are assigned in the exact same way first names are

# COMMAND ----------

f_m_name_schema = StructType([ \
    StructField("name",StringType(),True), \
    StructField("race",StringType(),True), \
    StructField("sex_at_birth",StringType(),True), \
    StructField("year",IntegerType(),True)
  ])
 
spdf_f_m_name = spark.createDataFrame(data = [('', '', '', None)], schema=f_m_name_schema)

# COMMAND ----------

spdf_f_m_name.write.format('delta')\
                   .mode('overwrite')\
                   .option('overwriteSchema',True)\
                   .save(f'{synthetic_gold_exchange}/FirstNameLookup/Delta')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Configure name data
# MAGIC
# MAGIC Since there were duplicate, unrepresentative issues with the data using UDFs to randomly assign names, I went with a more brute force method.
# MAGIC
# MAGIC 1. Get a surplus-sized list of randomized, proportional names 
# MAGIC 2. Format the data to include all relevant information, including race, sex (for fname/mname), and row number
# MAGIC 3. Add them (sql union) to a predefined spark dataframe
# MAGIC 4. In different cell, do a clever join to the original dataframe using rownumbers (defined previously using window functions) and supporting info like race/sex.

# COMMAND ----------

vitalstats = spark.read.format('csv')\
                       .option('inferSchema',True)\
                       .option('header',True)\
                       .load(f'{synthetic_gold_supporting}/Births/03_Complete/VitalStats_byYear_byState.csv')\
                       .filter(f'(State == "{stateExtended}")')

# COMMAND ----------

fname_probabilities = spark.read.format('csv')\
                           .option('inferSchema',True)\
                           .option('header',True)\
                           .load(f'{synthetic_gold_supporting}/Names/03_Complete/firstname_probabilities.csv')

min_year = fname_probabilities.agg(F.min('Year')).collect()[0][0]
max_year = fname_probabilities.agg(F.max('Year')).collect()[0][0]

# COMMAND ----------

# Get lists of all options
list_available_sexes = ['M','F']
list_available_races = ['White','Black or African American','Asian or Pacific Islander','American Indian or Alaska Native','Hispanic']
list_years = list(range(1895,max_year+1))

# For each year...
for year in list_years: 
  
  if year < 1920:
    state_max_births = int(vitalstats.filter(f'year == 1920').agg(F.max('Births')).collect()[0][0] / 2)
    
  else:
    state_max_births = int(vitalstats.filter(f'year == {year}').agg(F.max('Births')).collect()[0][0] / 2)
  
  # For each race...
  for race in list_available_races:

    # For each sex...
    for sex in list_available_sexes:

      # Load in data on firstnames.  Note we're still within the race for-loop so this is for each race/sex combo.
      cur_dict_fm_names = fname_probabilities.filter(f'(Sex == "{sex}") AND (Race == "{race}") AND (Year == "{year}")').toPandas()

      ###############################################################

      # Get randomized lists of firstnames/middlenames using their probabilities.  Computed identically.
      randomized_names = np.random.choice(cur_dict_fm_names['Name'], state_max_births, p=cur_dict_fm_names['Probability'])

      # Create dataframe and load in the data
      df_f_m_names = pd.DataFrame(list(randomized_names), columns=['name'])

      # Define race/sex fields
      df_f_m_names['race'] = race
      df_f_m_names['sex_at_birth'] = sex
      df_f_m_names['year'] = year

      # Convert to spark and union with predefined pyspark dataframe
      df_f_m_names.to_spark().createOrReplaceTempView('currentGroup')
      
      spark.sql(f'''
      
      INSERT INTO delta.`{synthetic_gold_exchange}/FirstNameLookup/Delta`
      TABLE currentGroup
      
      ''')
      
  print(f'Year {year} complete')


# COMMAND ----------

spark.sql(f'''

OPTIMIZE delta.`{synthetic_gold_exchange}/FirstNameLookup/Delta`
ZORDER BY (year)

''')
