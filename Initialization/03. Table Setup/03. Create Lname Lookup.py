# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Create Lname Lookup
# MAGIC
# MAGIC The purpose of this script is to create a bank of Lastnames.
# MAGIC As part of the birthing process, people are assigned names.
# MAGIC It would take too much processing to load in the name probability data & subsequently generate random names.
# MAGIC
# MAGIC In the new process, we hope to:
# MAGIC 1. Create a bank of lastnames with respective race info
# MAGIC 2. Load it in during birthing process
# MAGIC 3. Shuffle the data randomly
# MAGIC 4. Join it to new births by race/sex/F.row_number() within race/sex window partition.
# MAGIC
# MAGIC <b>This script focuses on generating the dataset used in Step 1.</b>
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
# MAGIC ### Last Name Data

# COMMAND ----------

l_name_schema = StructType([ \
    StructField("name",StringType(),True), \
    StructField("race",StringType(),True)
  ])
 
spdf_l_name = spark.createDataFrame(data = [('', '')], schema=l_name_schema)

# COMMAND ----------

spdf_l_name.write.format('delta')\
                   .mode('overwrite')\
                   .option('overwriteSchema',True)\
                   .save(f'{synthetic_gold_exchange}/LastNameLookup/Delta')

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

state_pop1920 =  vitalstats.filter('Year == 1920').select('Population').collect()[0][0]

# COMMAND ----------

lname_probabilities = spark.read.format('csv')\
                           .option('inferSchema',True)\
                           .option('header',True)\
                           .load(f'{synthetic_gold_supporting}/Names/03_Complete/lastname_probabilities.csv')


# COMMAND ----------

# Get lists of all options
list_available_races = ['White','Black or African American','Asian or Pacific Islander','American Indian or Alaska Native','Hispanic']

# For each race...
for race in list_available_races:

  # Load in data on firstnames.  Note we're still within the race for-loop so this is for each race/sex combo.
  cur_dict_fm_names = lname_probabilities.filter(f'(Race == "{race}")').toPandas()

  ###############################################################

  # Get randomized lists of firstnames/middlenames using their probabilities.  Computed identically.
  randomized_names = np.random.choice(cur_dict_fm_names['Name'], state_pop1920, p=cur_dict_fm_names['Probability'])

  # Create dataframe and load in the data
  df_f_m_names = pd.DataFrame(list(randomized_names), columns=['name'])

  # Define race/sex fields
  df_f_m_names['race'] = race

  # Convert to spark and union with predefined pyspark dataframe
  df_f_m_names.to_spark().createOrReplaceTempView('currentGroup')

  spark.sql(f'''

  INSERT INTO delta.`{synthetic_gold_exchange}/LastNameLookup/Delta`
  TABLE currentGroup

  ''')
  
  print(f'Completed for race: {race}')

