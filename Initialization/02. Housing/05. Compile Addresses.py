# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Compile Addresses
# MAGIC
# MAGIC ## Purpose
# MAGIC
# MAGIC I hope to accomplish the following:
# MAGIC <p></p>
# MAGIC
# MAGIC * Get a table of all addresses in our simulated state
# MAGIC
# MAGIC We have already compiled a list of all streets in our state along with ZIP information via Census's TIGER data.
# MAGIC Records in this dataset have a unique combination of ZIP, Streetname, Address Start range, Address End range.
# MAGIC
# MAGIC We will use the Data Profile estimates for housing units by zip to generate a (generous) list of addresses.
# MAGIC These generated addresses will later be assigned houses of varying types.  
# MAGIC See types below.
# MAGIC <p></p>
# MAGIC
# MAGIC * One-family house detached
# MAGIC * 50 or More Apartments
# MAGIC * Mobile Home or Trailer
# MAGIC * 10-19 Apartments
# MAGIC * 5-9 Apartments
# MAGIC * 20-49 Apartments
# MAGIC * One-family house attached
# MAGIC * 3-4 Apartments
# MAGIC * 2 Apartments
# MAGIC * N/A (GQ)
# MAGIC * Boat, RV, van, etc.
# MAGIC
# MAGIC ----------------------
# MAGIC
# MAGIC <p>Author: PJ Gibson</p>
# MAGIC <p>Create Date: 2022-01-14</p>
# MAGIC <p>Contact: peter.gibson@doh.wa.gov</p>
# MAGIC <p>Other Contact: pjgibson25@gmail.com</p>

# COMMAND ----------

# MAGIC %md
# MAGIC # 1.  Load in variables, libraries

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Run Global Variables Script
# MAGIC
# MAGIC This script contains important file paths and variables.
# MAGIC For the purposes of this script, it is only necessary that you have the following variables:
# MAGIC <p></p>
# MAGIC * <b>root_synthetic_gold</b> : path to root project directory (ex: "dbfs:/path/to/synthetic-gold" )

# COMMAND ----------

# DBTITLE 0,Run global variables script
# MAGIC %run "../../Python Global Variables"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Set Random Seed
# MAGIC
# MAGIC Per recommendation on Towards Data Science [linked here](https://towardsdatascience.com/stop-using-numpy-random-seed-581a9972805f), we use numpy's random.default_rng() to set our random seed.
# MAGIC This is the recommended practice as of this script's create date.

# COMMAND ----------

# DBTITLE 0,Set random seed
rng = np.random.default_rng( 42 )

# COMMAND ----------

# MAGIC %md
# MAGIC # 2.  Read & Wrangle

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Read Data

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Wrangled Housing (spark)
# MAGIC
# MAGIC PUMS data has been wrangled so that each row represents and individual house.
# MAGIC Each record (row) contains information on the house type, number bedrooms, and many other relevant fields.
# MAGIC Each record also has an assigned ZIP code.
# MAGIC
# MAGIC We read this in as a spark dataframe for the easier to do some initial wrangling.
# MAGIC
# MAGIC <p></p>
# MAGIC * More information on PUMS / the PUMA field [linked here](https://www.census.gov/content/dam/Census/library/publications/2021/acs/acs_pums_handbook_2021.pdf)

# COMMAND ----------

# Read in data
spdf_individual_housings = spark.read.format('csv').option('inferSchema',True).option('header',True).load(f'{root_synthetic_gold}/SupportingDocs/Housing/03_Complete/PUMS_with_zip.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Streets
# MAGIC
# MAGIC This data was taken by joining several census TIGER dataset tables.  
# MAGIC The most important aspects of the dataset include:
# MAGIC <p></p>
# MAGIC
# MAGIC * ZIP
# MAGIC * Street Name
# MAGIC * Address Range

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Read & Format

# COMMAND ----------

# Read in data
spdf_streets = spark.read.format('csv').option('inferSchema',True).option('header',True).load(f'{root_synthetic_gold}/SupportingDocs/Housing/03_Complete/TIGER_state_streets.csv')

# Drop rows if they have NA values in any of the following fields
spdf_streets = spdf_streets.dropna(subset=['FULLNAME','ZIP','FROMHN','TOHN'])

# Format the from and to as integers
for c in ['FROMHN', 'TOHN']:
  spdf_streets = spdf_streets.withColumn(c , F.col(c).cast("int"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Wrangle Data

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Streets - choose addresses

# COMMAND ----------

@udf(ArrayType(IntegerType(), True))
def get_range(start, end):
  '''
  Returns a column containing a list of all available ints (incrementing by incriment) between start and end.
  
  Args:
    start (pyspark Column): int64 type, nullable
    end (pyspark Column): int64 type, nullable
    increment (int): value to incriment by
  
  Returns:
    pyspark column: returning a list of values in between start and end
  '''
  if end is None or start is None:
    return np.nan
  
  else:
    smallest = np.min([start, end])
    largest = np.max([start, end])
    return list(range(smallest,largest+1,2))

# COMMAND ----------

# Get ranges of addresses for each street section using their bounds
spdf_streets = spdf_streets.withColumn('all_address_options', get_range(F.col('FROMHN'), F.col('TOHN')))

# Assign IDs to each road/street section
spdf_streets = spdf_streets.withColumn('road_section_id', F.monotonically_increasing_id())

# Create a row for every possible address in each street section
spdf_streets = spdf_streets.withColumn('address_num', F.explode(F.col('all_address_options')))

# Filter down the data
spdf_streets_wrangled = spdf_streets.select(['FULLNAME','ZIP','COUNTY_FIPS','address_num','road_section_id'])

# Drop out null address numbers
spdf_streets_wrangled = spdf_streets_wrangled.dropna(subset=['address_num'])

# Drop duplicates just in case
spdf_streets_wrangled = spdf_streets_wrangled.drop_duplicates(subset=['ZIP','FULLNAME','address_num'])

# Limit each road section to a max of 100 address values (limits ranges that have over 1000 values)
spdf_streets_wrangled = spdf_streets_wrangled.withColumn("road_id_rownum", F.row_number().over(Window.partitionBy("road_section_id").orderBy(F.rand(42))))\
                                             .filter("road_id_rownum < 101")\
                                             .drop('road_id_rownum')

# Scramble available addresses within ZIP code (randomized) and assign row number
spdf_streets_wrangled = spdf_streets_wrangled.withColumn("random_zip_id", F.row_number().over(Window.partitionBy("ZIP").orderBy(F.rand(42))))

# Save all addresses to a delta table
spdf_streets_wrangled.write.format('delta').mode('overwrite').save(f'{root_synthetic_gold}/SupportingDocs/Housing/03_Complete/all_possible_street_addresses/Delta')

# Convert to pandas
df_streets_wrangled = spdf_streets_wrangled.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Housing - ybl & # units

# COMMAND ----------

@udf(IntegerType())
def get_randint(start, end):
  '''
  Returns a column that chooses a value between 2 integer values allowing for NaN
  
  Args:
    start (koalas Series): int64 type, nullable
    end (koalas Series): int64 type, nullable
    
  Returns:
    koalas Series: returning a value in between start and end
  '''
  if end is None and start is not None:
    return start
  
  elif start is None and end is None:
    return np.nan
  
  else:
    return np.random.randint(low=start, high=end)
    
# Assign year built
spdf_individual_housings = spdf_individual_housings.withColumn('year_built', get_randint(F.col('YBL_range_start'), F.col('YBL_range_end')))

# Assign a number of units
spdf_individual_housings = spdf_individual_housings.withColumn('num_units', get_randint(F.col('BLD_range_start'), F.col('BLD_range_end')))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Housing - assign zip ID
# MAGIC
# MAGIC We will join the available addresses by ZIP & zip_id. 
# MAGIC This means that we need the equivilant of a zip_id for housing data.
# MAGIC The zip_id is a F.row_number() value for each group of zips.

# COMMAND ----------

# Scramble available addresses within ZIP code (randomized) and assign row number
spdf_individual_housings = spdf_individual_housings.withColumn("random_house_zip_id", F.row_number().over(Window.partitionBy("assigned_zip").orderBy(F.rand(42))))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Housing - Filter, Convert, Rename

# COMMAND ----------

# Filter housing data to specific columns & convert to pandas dataframe in 1 command
df_individual_housings = spdf_individual_housings.select('index','house_id','PUMA','assigned_zip','BDSP','RMSP','map_BLD','BLD_range_start','BLD_range_end','num_units','year_built','random_house_zip_id').toPandas()

# Rename columns
df_individual_housings = df_individual_housings.rename(columns={'index':'raw_id','PUMA':'puma','assigned_zip':'zip','BDSP':'num_beds','RMSP':'num_rooms','map_BLD':'bld_type'})

# Convert PUMA to string
df_individual_housings['zip'] = df_individual_housings['zip'].astype(str)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # 3. Assign street_id to houses
# MAGIC
# MAGIC The street_id will link a house not only to a streetname, but also to a individual row in our TIGER-derived dataset.
# MAGIC This row contains the streetname alongside street address bounds that we will use to assign unique values to our data.

# COMMAND ----------

# Rename zip column for pandas merge
df_streets_wrangled = df_streets_wrangled.rename(columns={'ZIP':'street_zip'})
df_streets_wrangled['street_zip'] = df_streets_wrangled['street_zip'].astype(int).astype(str)
df_streets_wrangled['address_num'] = df_streets_wrangled['address_num'].astype(int).astype(str)

# Merge together dataframes to get assigned addresses
df_combined = pandas.merge(df_individual_housings,df_streets_wrangled, how = 'inner', left_on = ['zip','random_house_zip_id'], right_on = ['street_zip','random_zip_id'])

# Filter to columns we will use
df_combined_filtered = df_combined[['house_id',
'puma',
'zip',
'num_beds',
'num_rooms',
'bld_type',
'BLD_range_start',
'BLD_range_end',
'num_units',
'year_built',
'FULLNAME',
'COUNTY_FIPS',
'address_num',
'road_section_id']]

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Save
# MAGIC
# MAGIC Overwrite our previous intermediary data.

# COMMAND ----------

pd.DataFrame(df_combined_filtered).to_csv(f'{root_synthetic_gold}/SupportingDocs/Housing/03_Complete/housing_nearly_finalized.csv', index=False)
