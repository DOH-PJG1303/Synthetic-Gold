# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Grandparent Generation Revised
# MAGIC
# MAGIC It is apparent that any small tweak in the grandparent generation could have a cascading effect to further generations.
# MAGIC Originally I wanted to have a population of 750,000 people born between 1900-1915.
# MAGIC But this isn't necissarily a good idea.
# MAGIC I'd rather have a population that reflects the age distribution by the year 1920, the year where our birthing prpocess will kick off.
# MAGIC
# MAGIC The following [link](https://www2.census.gov/library/publications/decennial/1920/volume-3/41084484v3ch09.pdf) (page 28/95) shows the age distribution of Washington's population for the year 1920.  
# MAGIC Using this information, we'll create a population.
# MAGIC
# MAGIC -------------
# MAGIC
# MAGIC Author(s): PJ Gibson
# MAGIC
# MAGIC Create Date: 2022-03-31
# MAGIC
# MAGIC Last Updated Date: 2022-03-31

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 0. Import global vars, libraries

# COMMAND ----------

# MAGIC %run "../../Python Global Variables"

# COMMAND ----------

from datetime import date

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 1. Initialize Data

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 1.1 Read in Population Count at 1920

# COMMAND ----------

# Read in our vital stats record, filtering to our state
population1920 = spark.read.format('csv')\
                      .option('inferSchema',True)\
                      .option('header',True)\
                      .load(f'{synthetic_gold_supporting}/Births/03_Complete/VitalStats_byYear_byState.csv')\
                      .filter(f'(State == "{stateExtended}") AND (Year == 1920)')\
                      .select('Population').collect()[0][0]

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 1.2 Init row number

# COMMAND ----------

spdf = pd.DataFrame(np.arange(1,population1920+1), columns=['row_num_initial']).to_spark()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 1.3 NULL columns
# MAGIC
# MAGIC * parents_partnership_id
# MAGIC * is_twin
# MAGIC * is_triplet
# MAGIC * is_jr
# MAGIC
# MAGIC
# MAGIC *Note that we define them as empty strings first, then later fill all empty strings with None values

# COMMAND ----------

spdf = spdf.withColumn('parents_partnership_id', F.lit(0))\
            .withColumn('is_twin', F.lit(False))\
            .withColumn('is_triplet', F.lit(False))\
            .withColumn('is_jr', F.lit(False))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 1.4 is_hetero

# COMMAND ----------

@udf(StringType(),True)
def sp_is_hetero( s ):
  '''
  Returns a value for an individual's sex at birth
  
  Args:
    s (pyspark Column): any type of column.  Not used.
  
  Returns:
    pyspark column: a value of "M" or "F" on a 50:50 split
  '''
  
  return str(np.random.choice([True, False], p=[0.995, 0.005]))

# COMMAND ----------

spdf = spdf.withColumn('is_hetero', sp_is_hetero(F.col('row_num_initial')))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 1.5 sex_at_birth

# COMMAND ----------

@udf(StringType(),True)
def sp_assign_sex( s ):
  '''
  Returns a value for an individual's sex at birth
  
  Args:
    s (pyspark Column): any type of column.  Not used.
  
  Returns:
    pyspark column: a value of "M" or "F" on a 50:50 split
  '''
  
  return str(np.random.choice(['M','F']))

# COMMAND ----------

spdf = spdf.withColumn('sex_at_birth', sp_assign_sex(F.col('row_num_initial')))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 1.6 race

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### 1.6.1 Get Census Breakdown
# MAGIC
# MAGIC Check out ACS breakdown for race in the most recent year.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Format API Call

# COMMAND ----------

# Enter most recent year for ASC1 data
year_most_recent_ASC = 2021

# Define base URL that we will query
base_url_acs = f'https://api.census.gov/data/{year_most_recent_ASC}/acs/acs1/profile'

# Define columns we will ask for in API call in dictionary
APIcolumns = {'DP05_0071PE':'percent_HispanicLatino',
              'DP05_0076PE':'percent_NonHispanicLatino',
              'DP05_0077PE':'percent_NonHispanicLatino_White',
              'DP05_0078PE':'percent_NonHispanicLatino_BlackOrAfricanAmerican',
              'DP05_0079PE':'percent_NonHispanicLatino_AmericanIndianOrAlaskaNative',
              'DP05_0080PE':'percent_NonHispanicLatino_Asian',
              'DP05_0081PE':'percent_NonHispanicLatino_NativeHawaiianPacificIslander',
              'DP05_0082PE':'percent_NonHispanicLatino_Other',
              'DP05_0083PE':'percent_NonHispanicLatino_TwoPlus'}

# Format our API requested columns into proper format
str_cols = ','.join(list(APIcolumns.keys()))

# Define output columns we'll use
columns_renamed = list(APIcolumns.values()) + ['State']

# Define our final URL used for the api query
url_API_call = f'{base_url_acs}?get={str_cols}&for=state:{stateFIPS}'

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Perform API Request
# MAGIC
# MAGIC Note: As described in the [linked ACS API Handbook pdf](https://www.census.gov/content/dam/Census/library/publications/2020/acs/acs_api_handbook_2020_ch02.pdf):
# MAGIC
# MAGIC > Any user may query small quantities of data with minimal restrictions (up to 50 variables in a single query, and up to 500 queries per IP address per day). However, more than 500 queries per IP address per day requires that you register for an API key."
# MAGIC
# MAGIC Here, we'll take advantage of that because hiding a census key that is databricks accessible is rather difficult in a public Github repository.

# COMMAND ----------

# Perform API get-request
response_census = requests.get(url_API_call)

# Validate our api call
validate_api_request(response_census)

# Parse the API text output into pandas dataframe, rename the columns
df_census = pd.DataFrame(json.loads(response_census.text), dtype = str)
df_census.columns = columns_renamed

# Only take the values we want, not the first row that only contains old column names
df_census = df_census.iloc[1:2].astype(float)

# Create new column combining percent asian , percent hawaiian/pacific islander
df_census['percent_NonHispanicLatino_AsianOrPacificIslander'] = df_census['percent_NonHispanicLatino_Asian'] + df_census['percent_NonHispanicLatino_NativeHawaiianPacificIslander']

# Columns we care about
cols_output = ['percent_NonHispanicLatino_White',
               'percent_HispanicLatino',
               'percent_NonHispanicLatino_AsianOrPacificIslander',
               'percent_NonHispanicLatino_BlackOrAfricanAmerican',
               'percent_NonHispanicLatino_AmericanIndianOrAlaskaNative']

# Define our numpy race list
raceList = df_census[cols_output].iloc[0].to_numpy()

# Normalize our list to 100.  
### 2 or more races / Other make up the remaining bit.  Since they don't easily fit into our simulation, we exclude them and normalize.
normalizedRaceList = custom_normalize(raceList)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### 1.6.2 Assign Probability Column

# COMMAND ----------

spdf = spdf.withColumn('proba_races', F.array([F.lit(x) for x in normalizedRaceList]))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### 1.6.3 Assign Race

# COMMAND ----------

@udf(StringType(),True)
def sp_assign_race( s ):
  '''
  Returns a value for an individual's race at birth based on probability of input column "s"
  
  Args:
    s (pyspark Column): column indicating probability of being each race listed in function.
  
  Returns:
    pyspark column: a value of race by probalistic value
  '''
  options = ['White','Hispanic','Asian or Pacific Islander','Black or African American','American Indian or Alaska Native']
  
  return str(np.random.choice(options, p=s))

# COMMAND ----------

spdf = spdf.withColumn('race', sp_assign_race(F.col('proba_races')))\
           .drop('proba_races')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 1.7 age
# MAGIC
# MAGIC Breakdown of population by age for the year 1920 found at the [link here (PDF page 4/40)](https://www2.census.gov/library/publications/decennial/1920/volume-3/41084484v3ch01.pdf).
# MAGIC Note that the second value of 0.093 doesn't match what you'd expect to see (11.6 - 2.4 = 9.2% = 0.092).
# MAGIC However, we needed an extra tenth of a percent (0.001) to reach a normalized 1.0 sum for all probabilities.
# MAGIC So I decided to add it to that group, kind of at random.
# MAGIC
# MAGIC <b>This data represents the age breakdown of the United States as a whole.</b>
# MAGIC Breakdowns by age for other states specifically can be found in similar PDFs, but need to be entered manually.

# COMMAND ----------

@udf(IntegerType(),True)
def sp_assign_age( s ):
  '''
  Returns an array of arrays containing information on the new births
  
  Args:
    s (pyspark Column): any type of column.  Not used.
  
  Returns:
    pyspark column: a value of age represented by 1920 cencus population of WA
  '''
  lowerbound = np.array([ 0, 1, 5, 10, 15, 20, 45])
  upperbound = np.array([ 0, 4, 9, 14, 19, 44, 80])
  
  prob_group = [0.024, 0.093, 0.106, 0.099, 0.099, 0.390, 0.189]
  
  age_bracket_index = np.random.choice(np.arange(0,len(prob_group)) , p = prob_group)
  
  return int(np.random.choice(np.arange(lowerbound[age_bracket_index] , upperbound[age_bracket_index] + 1)))

# COMMAND ----------

spdf = spdf.withColumn('age', sp_assign_age(F.col('row_num_initial')))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 1.8 Save Intermediary Location!

# COMMAND ----------

spdf.write.format('delta')\
     .mode('overwrite')\
     .option('overwriteSchema',True)\
     .save(f'{root_synthetic_gold}/Data/UNFINISHED/birth_prenames/Delta')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 2. Part 2 field assignments

# COMMAND ----------

spdf = spark.read.format('delta').load(f'{root_synthetic_gold}/Data/UNFINISHED/birth_prenames/Delta')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 2.1 dob
# MAGIC
# MAGIC DOBs selected randomly within range of the year the individual was born in.

# COMMAND ----------

@udf(StringType(),True)
def sp_assign_dob( age ):
  '''
  Returns an array of arrays containing information on the new births
  
  Args:
    age (pyspark Column): Column representing person age.
        year of birth determined by age and the current year within simulation, 1920
  
  Returns:
    pyspark column: a valid date that gives them their respective age.
  '''
  year_of_birth = 1920 - int(age)
  
  start_date = date(year_of_birth, 1, 1)
  end_date = date((year_of_birth+1), 1, 1)
  
  time_between_dates = end_date - start_date
  days_between_dates = time_between_dates.days
  
  days_after_jan1 = int(np.random.choice(np.arange(0,days_between_dates)))
  time_delta = pandas.Timedelta(days_after_jan1, unit = 'D')
  dob = time_delta + pandas.Timestamp(f'{year_of_birth}-01-01 00:00:00')
  
  return str(dob)[:10]

# COMMAND ----------

spdf = spdf.withColumn('dob', sp_assign_dob(F.col('age')))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 2.2 Names

# COMMAND ----------

# Prep spdf for name assignment by giving unique row_numbers to match on partitioned intentionally by characteristic.
spdf = spdf.withColumn('yob', F.col('dob').substr(1,4))\
           .withColumn('yob_match', F.when(F.col('yob') < 1895, F.lit(1895)).otherwise(F.col('yob')))\
           .withColumn('fname_rownum', F.row_number().over(Window.partitionBy(['sex_at_birth','race','yob']).orderBy(F.rand(1))))\
           .withColumn('lname_rownum', F.row_number().over(Window.partitionBy(['race']).orderBy(F.rand(2))))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### 2.2.1 First Name

# COMMAND ----------

# Define our lookup table and format column names
fnameLookup = spark.read.format('delta').load(f'{synthetic_gold_exchange}/FirstNameLookup/Delta')\
                   .withColumn('fname_rownum', F.row_number().over(Window.partitionBy('year','race','sex_at_birth').orderBy(F.rand(1))))\
                   .withColumnRenamed('name','first_name')\
                   .withColumnRenamed('year','yob_match')

# Join back to spdf for assignment
spdf = spdf.join(fnameLookup, on=['yob_match','race','sex_at_birth','fname_rownum'], how='inner')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### 2.2.2 Middle Name
# MAGIC
# MAGIC Middle names are assigned in the exact same way first names are

# COMMAND ----------

@udf(StringType())
def pick_middlename( fname, mname1, mname2, mname3, mname4, mname5 ):
  '''
  Selects the first middlename that is not the firstname and returns it
  
  Inputs:
    fname - str, pyspark column representing first_name
    mname1 - str, pyspark column representing middle_name option 1
    mname2 - str, pyspark column representing middle_name option 2
    mname3 - str, pyspark column representing middle_name option 3
    mname4 - str, pyspark column representing middle_name option 4
    mname5 - str, pyspark column representing middle_name option 5
    
  Outputs:
    StringType column output
  '''
  
  for mname in [mname1, mname2, mname3, mname4, mname5]:
    if mname != fname:
      return mname

# COMMAND ----------

# Create a couple of mnameLookups.  We're going to try to ensure that firstname does not equal middlename
mnameLookup = fnameLookup.withColumn('fname_rownum', F.row_number().over(Window.partitionBy('yob_match','race','sex_at_birth').orderBy(F.rand(2))))\
                         .withColumnRenamed('first_name','middle_name1')

mnameLookup2 = mnameLookup.withColumn('fname_rownum', F.row_number().over(Window.partitionBy('yob_match','race','sex_at_birth').orderBy(F.rand(3))))\
                          .withColumnRenamed('middle_name1','middle_name2')
mnameLookup3 = mnameLookup.withColumn('fname_rownum', F.row_number().over(Window.partitionBy('yob_match','race','sex_at_birth').orderBy(F.rand(4))))\
                          .withColumnRenamed('middle_name1','middle_name3')
mnameLookup4 = mnameLookup.withColumn('fname_rownum', F.row_number().over(Window.partitionBy('yob_match','race','sex_at_birth').orderBy(F.rand(5))))\
                          .withColumnRenamed('middle_name1','middle_name4')
mnameLookup5 = mnameLookup.withColumn('fname_rownum', F.row_number().over(Window.partitionBy('yob_match','race','sex_at_birth').orderBy(F.rand(6))))\
                          .withColumnRenamed('middle_name1','middle_name5')


spdf = spdf.join(mnameLookup, on=['yob_match','race','sex_at_birth','fname_rownum'], how='inner')\
           .join(mnameLookup2, on=['yob_match','race','sex_at_birth','fname_rownum'], how='inner')\
           .join(mnameLookup3, on=['yob_match','race','sex_at_birth','fname_rownum'], how='inner')\
           .join(mnameLookup4, on=['yob_match','race','sex_at_birth','fname_rownum'], how='inner')\
           .join(mnameLookup5, on=['yob_match','race','sex_at_birth','fname_rownum'], how='inner')


spdf = spdf.withColumn('middle_name', pick_middlename(F.col('first_name'), F.col('middle_name1'), F.col('middle_name2'), F.col('middle_name3'), F.col('middle_name4'), F.col('middle_name5')))


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### 2.2.3 Last Name

# COMMAND ----------

# Define our lookup table and format column names
lnameLookup = spark.read.format('delta').load(f'{synthetic_gold_exchange}/LastNameLookup/Delta')\
                   .withColumn('lname_rownum', F.row_number().over(Window.partitionBy('race').orderBy(F.rand(99))))\
                   .withColumnRenamed('name','last_name')

# Join back to spdf for assignment
spdf = spdf.join(lnameLookup, on=['race','lname_rownum'], how='inner')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 2.3 SSN Assignments
# MAGIC
# MAGIC Ordered by DOB so earlier births are first

# COMMAND ----------

# Read in SSN data, filtering to is_used = False and ensuring we only read in data for 2011
spdf_ssn = spark.read.format('delta').load(f'{synthetic_gold_exchange}/SSNPool/Delta')\
                .filter(F.col('is_used') == False)\
                .filter(F.col('process_end_date') == 2011)\
                .withColumn('row_num_ssn', F.row_number().over(Window.partitionBy(F.lit(1)).orderBy(F.asc('index'))))\
                .select('row_num_ssn','ssn')

# prep a column to match SSN values to
spdf = spdf.withColumn('row_num_ssn', F.row_number().over(Window.partitionBy(F.lit(1)).orderBy(F.asc('dob'), F.asc('row_num_initial'))))

# Join
spdf = spdf.join(spdf_ssn, on='row_num_ssn', how='inner')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 2.4 Event Year

# COMMAND ----------

spdf = spdf.withColumn('event_year', F.col('dob').substr(1,4))\
           .withColumn('event_year', F.col('event_year').cast(IntegerType()))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 2.5 Cleanup/Formatting

# COMMAND ----------

spdf = spdf.replace('',None)

# COMMAND ----------

final_cols = [ 'ssn',
 'parents_partnership_id',
  'first_name',
 'middle_name',
 'last_name',
 'dob',
 'sex_at_birth',
  'race',
  'is_hetero',
  'is_twin',
 'is_triplet',
 'is_jr',
 'event_year']

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 2.6 Saving

# COMMAND ----------

spdf.select(final_cols).write.format('delta')\
                       .mode('overwrite')\
                       .option('overwriteSchema',True)\
                       .save(f'{synthetic_gold_exchange}/Births/Delta')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 3. Update SSNPool table

# COMMAND ----------

spark.sql(f'''

MERGE INTO delta.`{synthetic_gold_exchange}/SSNPool/Delta` t
USING delta.`{synthetic_gold_exchange}/Births/Delta` s
ON t.ssn = s.ssn
WHEN MATCHED THEN 
     UPDATE SET t.is_used = True
     
     ''').display()

# COMMAND ----------


