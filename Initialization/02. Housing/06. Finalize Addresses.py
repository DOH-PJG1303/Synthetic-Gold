# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Finalize Addresses
# MAGIC
# MAGIC
# MAGIC ## Purpose
# MAGIC
# MAGIC I hope to accomplish the following:
# MAGIC <p></p>
# MAGIC
# MAGIC * Assign shared addresses to aggregate living facilities
# MAGIC * Assign unit/suite/apt numbers to agg living facilities
# MAGIC
# MAGIC As of this point, we addresses for (nearly) all of the housing units.
# MAGIC The only disparity is with aggregate living facilities.
# MAGIC Aggregate living facilities contain housing units that share the same address number and address fullname, but have different unit/suite/apt numbers.
# MAGIC We hope to group these aggregate living facilities into their intended correct sizes.
# MAGIC
# MAGIC Examples of aggregate living facilities include:
# MAGIC <p></p>
# MAGIC
# MAGIC * 50 or More Apartments
# MAGIC * Mobile Home or Trailer
# MAGIC * 10-19 Apartments
# MAGIC * 5-9 Apartments
# MAGIC * 20-49 Apartments
# MAGIC * 3-4 Apartments
# MAGIC
# MAGIC The route that we will take is fairly brute-force.  
# MAGIC This won't be that efficient, but is the best route in my opinion.
# MAGIC
# MAGIC
# MAGIC ----------------------
# MAGIC
# MAGIC <p>Author: PJ Gibson</p>
# MAGIC <p>Create Date: 2022-01-28</p>
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
# MAGIC # 2.  Read

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Read Data

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Wrangled Housing
# MAGIC
# MAGIC PUMS data has been wrangled so that each row represents and individual house.
# MAGIC More information on PUMS / the PUMA field [linked here](https://www.census.gov/content/dam/Census/library/publications/2021/acs/acs_pums_handbook_2021.pdf).
# MAGIC Each record (row) contains information on the house type, number bedrooms, and many other relevant fields.
# MAGIC Following previous wrangling, each house also has a:
# MAGIC <p></p>
# MAGIC
# MAGIC * zip
# MAGIC * street name*
# MAGIC * street address*
# MAGIC
# MAGIC <p></p>
# MAGIC \* for aggregate living facilities, these two fields may change 

# COMMAND ----------

# DBTITLE 0,Read - PUMS housing data
# Read in our data
df_housing = pd.read_csv(f'{root_synthetic_gold}/SupportingDocs/Housing/03_Complete/housing_nearly_finalized.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Geocorr

# COMMAND ----------

# Read in our dataset
df_Geocorr = pd.read_csv(f'{root_synthetic_gold}/SupportingDocs/Housing/01_Raw/GeoCorr_mapping_hus.csv')

# Koalas has no "skiprows" parameter so we need to do this manually
df_Geocorr = df_Geocorr[1:].reset_index()

# Convert afact to float
df_Geocorr['afact'] = df_Geocorr['afact'].astype('float64')

# Make number of houses in zip an integer
df_Geocorr['hus10'] = df_Geocorr['hus20'].astype(int)

# Remove records with no houses
df_Geocorr = df_Geocorr.query('hus20 > 0')

# Add city column
df_Geocorr['city'] = df_Geocorr['ZIPName'].str.split(',', n=2, expand = True)[0]

spdf_Geocorr = df_Geocorr.to_spark()

spdf_Geocorr.createOrReplaceTempView('spdf_Geocorr')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Phone AC lookup
# MAGIC
# MAGIC Note that this data was downloaded from the following website, [linked here](https://www.unitedstateszipcodes.org/zip-code-database/)
# MAGIC   * <b>Note that you agree to use this data for non-commercial means when you do this.</b>

# COMMAND ----------

# Read in data
df_ac_map = pd.read_csv(f"{root_synthetic_gold}/SupportingDocs/Phone/01_Raw/USZIPCODES_map_zip_ac.csv")

# Filter to populated regions in WA
df_ac_map = df_ac_map.query(f'(state == "{state}") AND (area_codes != "None")')[['zip','area_codes']]

# Convert to pyspark
spdf_ac_map = df_ac_map.to_spark()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # 3. Wrangle

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Clean field(s)

# COMMAND ----------

# Clean Puma field to be 0-padded
df_housing['puma'] = df_housing['puma'].astype(str).str.zfill(5)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Assign new fields

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Assign State

# COMMAND ----------

df_housing['state'] = state

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Assign County

# COMMAND ----------

# Grab 'FIPS':'COUNTYNAME' dict for all counties within your state
dict_FIPS_crosswalk = {row['FIPS']:row['COUNTYNAME'] for row in fips_crosswalk.collect()}

# Apply the replace transformation to get all of the county names
df_housing['county_name'] = df_housing['COUNTY_FIPS'].astype(pandas.Int64Dtype()).astype(str).replace(dict_FIPS_crosswalk)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Assign City
# MAGIC
# MAGIC Join our data with geocorr on puma, zip, and state to get the correct city

# COMMAND ----------

spdf_housing = df_housing.to_spark()
spdf_housing = spdf_housing.withColumn('zip', F.col('zip').astype(StringType()))

spdf_housing.createOrReplaceTempView('spdf_housing')

# COMMAND ----------

spdf_housing = spark.sql(f'''

SELECT {','.join( [f'h.{col}' for col in spdf_housing.columns])},
       g.city
from spdf_housing h
LEFT JOIN spdf_Geocorr g
ON (g.puma12 = h.puma) AND (g.zcta = h.zip) AND (g.stab = h.state)

''')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Assign Area Code

# COMMAND ----------

spdf_housing = spdf_housing.join(spdf_ac_map, on='zip', how = 'left')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Create helper function
# MAGIC
# MAGIC Since a zip can have multiple area codes, we'll need to select only one in instances where multiple are available.

# COMMAND ----------

@udf(StringType())
def select_ac( s ):
  '''
  Function returns a single area code given n comma seperated codes, each with a 1/n probability.
  
  Args:
    s, string: should be the column "area_codes" from the phone ac lookup data.  Contains some individual ac values, and others comma seperated, no spaces.
    
  Returns:
    string
  '''
  
  if s is None:
    return None
  else:
    return str(np.random.choice(s.split(',')))
  

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Apply function
# MAGIC
# MAGIC Note that we're only going to apply this function to a handful of specific records, each representing their zip/road section.
# MAGIC This way, houses directly next to each other within the same zip will be forced to have the same area code, which makes sense inherently in my head.
# MAGIC More of an art than a real science.

# COMMAND ----------

# Find out the row number within each group.  We will eventually only take the first value
spdf_AC_representing_houses = spdf_housing.withColumn('rownum', F.row_number().over(Window.partitionBy(['zip','road_section_id']).orderBy(F.rand(1))))\
                                          .filter('rownum = 1')\
                                          .drop('rownum')

# Apply function to select AC when multiple are available.
spdf_AC_representing_houses = spdf_AC_representing_houses.withColumn('area_code', select_ac(F.col('area_codes')))

# Filter down columns so the re-join doesn't reproduce cols
spdf_AC_representing_houses = spdf_AC_representing_houses.select(['zip','road_section_id','area_code'])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Join data back, fill NA
# MAGIC
# MAGIC Since we rely on the area code to assign phones, it is necissary that we fill it out.
# MAGIC It's not scientific, but we'll give any null values a value of cooresponding to the most populous area code.

# COMMAND ----------

# Join back up and drop the old ACs field that can have multiple.  Now we only have the "area_code" (singular) field.
spdf_housing = spdf_housing.join(spdf_AC_representing_houses, how = 'left', on=['zip','road_section_id']).drop('area_codes')

# COMMAND ----------

# Find the area code with the most houses in it.  Will use to fill NA values
most_populous_areaCode = spdf_housing.filter(F.col('area_code').isNotNull())\
                                     .groupBy('area_code')\
                                     .count()\
                                     .orderBy(F.desc('count'))\
                                     .select('area_code').collect()[0][0]

# Fill the NA values
spdf_housing = spdf_housing.withColumn('area_code', F.when(F.col('area_code').isNull() , F.lit(most_populous_areaCode)).otherwise(F.col('area_code')))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Split into single housing & agg living

# COMMAND ----------

# Create subset where multiple units in house
spdf_agg = spdf_housing.filter('num_units > 1')

# Create subset where individual housing
spdf_individual_housings = spdf_housing.filter('(num_units <= 1) OR (num_units IS NULL)')

# Print outputs
print('\n')
print(f'{spdf_individual_housings.count()} : count of individual living housings')
print(f'{spdf_agg.count()} : count of aggregate living housings')
print('\n')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # 4. Aggregate agg housings

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Determine groups of aggregate living situations

# COMMAND ----------

# Find out how many buildings of the same type exist within a zip code
spdf_agg = spdf_agg.withColumn("ct_sameBLD_within_zip", F.count('*').over(Window.partitionBy(['zip','bld_type'])))

# Using the average unit number within a building type (3-4 units would be avg of 3.5) and determine number of groups based on this
spdf_agg = spdf_agg.withColumn('ntile_groups', F.col('ct_sameBLD_within_zip') / ((F.col('BLD_range_start') + F.coalesce(F.col('BLD_range_end'), F.col('BLD_range_start')))/2))

# Get a given row's percent rank within their building/zip group
spdf_agg = spdf_agg.withColumn("percrank_sameBLD_within_zip", F.percent_rank().over(Window.partitionBy(['zip','bld_type']).orderBy(F.asc('year_built'),F.rand(1))))

# Find out which group a given record belongs to
spdf_agg = spdf_agg.withColumn('group', F.floor(F.col('ntile_groups') * F.col('percrank_sameBLD_within_zip')) + 1)

# Find out how many units in a given group
spdf_agg = spdf_agg.withColumn("group_count", F.count('*').over(Window.partitionBy(['zip','bld_type','group'])))

# Find out the row number within each group.  We will eventually only take the first value
spdf_agg = spdf_agg.withColumn('group_num', F.row_number().over(Window.partitionBy(['zip','bld_type','group']).orderBy(F.asc('year_built'),F.rand(1))))

# Create a temp view
spdf_agg.createOrReplaceTempView('spdf_agg')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Wrangle *special* records
# MAGIC
# MAGIC Records that will preserve their addresses will be analyzed further

# COMMAND ----------

@udf(StringType())
def assign_unit_type( bld_type ):
  '''
  Function returns a unit type (ex: unit, ste, apt, spc, fl, lot) given the building type.
  
  Args:
    bld_type, string: should be the column "bld_type"
    
  Returns:
    string
  '''
  
  if (bld_type == "Mobile Home or Trailer"):
    return str(np.random.choice(['LOT','SPC'], p = [0.7,0.3]))
  
  elif (bld_type == "2 Apartments"):
    return str(np.random.choice(['FL','STE','APT'], p = [0.4,0.3,0.3]))
  
  else:
    return str(np.random.choice(['APT','UNIT','STE'], p = [0.45,0.45,0.1]))
  

# COMMAND ----------

# Define our subset
spdf_specials = spdf_agg.filter('group_num == 1')

# Enact our function on it
spdf_specials = spdf_specials.withColumn('unit_type', assign_unit_type(F.col('bld_type')))

# create a temp view
spdf_specials.createOrReplaceTempView('spdf_kept_addresses')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Join up Data

# COMMAND ----------

new_agg_facs = spark.sql('''

WITH k as (

  SELECT
      house_id
      , road_section_id
      , FULLNAME
      , address_num
      , unit_type
      , group_num
      , zip
      , bld_type
      , COUNTY_FIPS
      , puma
      , group
      , county_name
      , area_code
      , city
      , state
  FROM spdf_kept_addresses
  WHERE group_num = 1

)

SELECT 
  k.house_id
  ,k.road_section_id
  
  ,s.num_beds
  ,s.num_rooms
  ,s.bld_type
  ,s.BLD_range_start
  ,s.BLD_range_end
  ,s.group_count as num_units
  ,s.year_built
  
  ,k.FULLNAME
  ,k.address_num
  ,k.unit_type
  ,s.group_num as unit_number
  
  ,k.zip
  ,k.county_name
  ,k.area_code
  ,k.city
  ,k.state
  ,k.COUNTY_FIPS
  ,k.puma

FROM spdf_agg s
INNER JOIN k 
ON ((s.zip = k.zip) AND (s.bld_type = k.bld_type) AND (k.group = s.group))


''')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # 5. Rejoin all data

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Format individual housing columns
# MAGIC
# MAGIC We added a new column, unit_number, to each of the newly created agg facilities.  
# MAGIC We need to add this same column represented by NULL values to individual housings for the union.

# COMMAND ----------

spdf_individual_housings = spdf_individual_housings.withColumn('unit_number', F.lit(None))\
                                                   .withColumn('unit_type', F.lit(None))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Join data

# COMMAND ----------

allfacs = new_agg_facs.unionByName(spdf_individual_housings)\
                      .withColumnRenamed('house_id','building_id')\
                      .withColumn('house_id', F.monotonically_increasing_id())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # 6. Assign Landline
# MAGIC
# MAGIC For roughly 95% of houses, we'll add a landline.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Read in phone data

# COMMAND ----------

# Read in data
phone_pool = spark.read.format('delta').load(f"{root_synthetic_gold}/SupportingDocs/Phone/03_Complete/phone_lookup/Delta")

# Create entire phone number column
phone_pool = phone_pool.withColumn('phone_whole', F.concat(F.col('ac'),F.col('phone_suffix')))

# Create row to match on, partitioned by area code
phone_pool = phone_pool.withColumn('match_row', F.row_number().over(Window.partitionBy('ac').orderBy(F.rand(1))))

# Create temp view
phone_pool.createOrReplaceTempView('phone_pool')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Wrangle housing tiny bit
# MAGIC
# MAGIC All we need is an extra column to join the phone data on

# COMMAND ----------

# Creat row to match on, partitioned by area code
###### NULL out 5% of the matching_row column so populates as null address.
allfacs = allfacs.withColumn('match_row', F.row_number().over(Window.partitionBy('area_code').orderBy(F.rand(2))))\
                 .withColumn('match_row', F.when(F.rand(3) >= 0.95, None).otherwise(F.col('match_row')))

# Create temp view
allfacs.createOrReplaceTempView('allfacs')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Join together

# COMMAND ----------

allfacs = spark.sql(f'''


SELECT {','.join([f'a.{col}' for col in allfacs.columns])},
       p.phone_whole as phone_landline
FROM allfacs a
LEFT JOIN phone_pool p
ON ((p.ac = a.area_code) AND (p.match_row = a.match_row))


''')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # 7. Save Output

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Format

# COMMAND ----------

output_columns_ordered = ['house_id',
'building_id',
'road_section_id',
'bld_type',
'num_units',
'num_beds',
'num_rooms',
'year_built',
'address_number',
'address_street',
'unit_type',
'unit_number',
'zip',
'city',
'county_name',
'county_fips',
'puma',
'area_code',
'phone_landline',
'state']


allfacs = allfacs.withColumnRenamed('FULLNAME','address_street')\
                 .withColumnRenamed('address_num','address_number')\
                 .withColumnRenamed('COUNTY_FIPS','county_fips')\
                 .withColumn('county_fips', F.col('county_fips').astype(IntegerType()).astype(StringType()))\
                 .drop('BLD_range_start')\
                 .drop('BLD_range_end')\
                 .select(output_columns_ordered)\
                 .withColumn('is_occupied', F.lit(False))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Save

# COMMAND ----------

allfacs.write.format('delta')\
             .mode('overwrite')\
             .option('overwriteSchema',True)\
             .save(f'{synthetic_gold_exchange}/HousingLookup/Delta')

# COMMAND ----------


