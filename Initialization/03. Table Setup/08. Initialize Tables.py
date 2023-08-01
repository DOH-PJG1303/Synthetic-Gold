# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Initialize Tables
# MAGIC
# MAGIC The purpose of this file is to initialize some of the tables used in other "Initial Setup" notebooks.
# MAGIC
# MAGIC -------------
# MAGIC
# MAGIC Author(s): PJ Gibson
# MAGIC
# MAGIC Create Date: 2022-04-05
# MAGIC
# MAGIC Last Updated Date: 2022-04-26

# COMMAND ----------

# MAGIC %run "../../Python Global Variables"

# COMMAND ----------

# MAGIC %run "../../Functions/State Management Functions"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # 1. Exchange Tables

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Partnership Lookup

# COMMAND ----------

save_location = f'{synthetic_gold_exchange}/PartnershipLookup/Delta'

my_schema = StructType([ \
    StructField("partnership_id",IntegerType(),True), \
    StructField("sexual_preference",IntegerType(),True), \
    StructField("partner1_ssn",StringType(),True), \
    StructField("partner1_race",StringType(),True), \
    StructField("partner1_age",IntegerType(),True), \
    StructField("partner2_ssn",StringType(),True),\
    StructField("partner2_race",StringType(),True), \
    StructField("partner2_first_name",StringType(),True),\
    StructField("partner2_middle_name",StringType(),True), \
    StructField("adopted_last_name",StringType(),True), \
    StructField("num_singletons",IntegerType(),True), \
    StructField("num_twins",IntegerType(),True),\
    StructField("num_triplets",IntegerType(),True), \
    StructField("num_total_children",IntegerType(),True), \
    StructField("has_jr",BooleanType(),True), \
    StructField("event_year",IntegerType(),True)
  ])
 
spdf_intialize = spark.createDataFrame(data = [( 0, 0, '', '', 0, '', '', '', '', '', 0, 0, 0, 0, False, 1800)], schema= my_schema)

spdf_intialize.write.format('delta').mode('overwrite').option('overwriteSchema',True).save(save_location)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## PartnershipEvents

# COMMAND ----------

save_location = f'{synthetic_gold_exchange}/PartnershipEvents/Delta'

my_schema = StructType([ \
    StructField("partnership_id",IntegerType(),True), \
    StructField("ssn",StringType(),True), \
    StructField("partner_number",IntegerType(),True),\
    StructField("adopted_last_name",StringType(),True), \
    StructField("adopted_house_id",LongType(),True), \
    StructField("event_year",IntegerType(),True)
  ])
 
spdf_intialize = spark.createDataFrame(data = [( 0, '', 0, '', 0, 0)], schema= my_schema)

spdf_intialize.write.format('delta').mode('overwrite').option('overwriteSchema',True).save(save_location)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## PhoneMobileEvents

# COMMAND ----------

save_location = f'{synthetic_gold_exchange}/PhoneMobileEvents/Delta'

my_schema = StructType([ \
    StructField("ssn",StringType(),True), \
    StructField("phone_mobile",StringType(),True), \
    StructField("event_year",IntegerType(),True)
  ])
 
spdf_intialize = spark.createDataFrame(data = [( '', '', 0)], schema= my_schema)

spdf_intialize.write.format('delta').mode('overwrite').option('overwriteSchema',True).save(save_location)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## HousingEvents

# COMMAND ----------

save_location = f'{synthetic_gold_exchange}/HousingEvents/Delta'

my_schema = StructType([
    StructField("ssn",StringType(),True), \
    StructField("old_house_id",LongType(),True),\
    StructField("new_house_id",LongType(),True),\
    StructField("event_type",IntegerType(),True), \
    StructField("event_year",IntegerType(),True)
  ])
 
spdf_intialize = spark.createDataFrame(data = [( '', 0, 0, 0, 0)], schema= my_schema)

spdf_intialize.write.format('delta').mode('overwrite').option('overwriteSchema',True).save(save_location)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## EmailEvents

# COMMAND ----------

save_location = f'{synthetic_gold_exchange}/EmailEvents/Delta'

my_schema = StructType([
    StructField("ssn",StringType(),True), \
    StructField("email",StringType(),True),\
    StructField("secondary_email",StringType(),True),\
    StructField("event_year",IntegerType(),True)
  ])
 
spdf_intialize = spark.createDataFrame(data = [( '', '', '', 0)], schema= my_schema)

spdf_intialize.write.format('delta').mode('overwrite').option('overwriteSchema',True).save(save_location)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Mortality

# COMMAND ----------

save_location = f'{synthetic_gold_exchange}/Mortality/Delta'

my_schema = StructType([ \
    StructField("ssn",StringType(),True), \
    StructField("age_at_death",IntegerType(),True), \
    StructField("event_year",IntegerType(),True)
  ])
 
spdf_intialize = spark.createDataFrame(data = [( '', 0, 0)], schema= my_schema)

spdf_intialize.write.format('delta').mode('overwrite').option('overwriteSchema',True).save(save_location)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## PrePartnerships

# COMMAND ----------

save_location = f'{synthetic_gold_exchange}/PrePartnerships/Delta'

my_schema = StructType([ \
      StructField("ssn",StringType(),True), \
      StructField("first_name",StringType(),True), \
      StructField("middle_name",StringType(),True), \
      StructField("last_name",StringType(),True), \
      StructField("dob",StringType(),True), \
      StructField("age",IntegerType(),True), \
      StructField("sex_at_birth",StringType(),True), \
      StructField("race",StringType(),True), \
      StructField("zip",StringType(),True), \
      StructField("fips",StringType(),True), \
      StructField("house_id",LongType(),True), \
      StructField("ever_partnered",BooleanType(),True), \
      StructField("will_marry",BooleanType(),True), \
      StructField("random_bit",IntegerType(),True), \
      StructField("is_hetero",StringType(),True), \
      StructField("sexual_preference",IntegerType(),True), \
      StructField("pairing_number",IntegerType(),True), \
      StructField("partner_number",IntegerType(),True), \
      StructField("event_year",IntegerType(),True), \
  ])
 
spdf_intialize = spark.createDataFrame(data = [( '', '', '', '', '', 0, '', '', 0, 0, 0, False, False, 0, '', 0, 0, 0, 0)], schema= my_schema)

spdf_intialize.write.format('delta').mode('overwrite').option('overwriteSchema',True).save(save_location)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## PreBirths

# COMMAND ----------

save_location = f'{synthetic_gold_exchange}/PreBirths/Delta'

my_schema = StructType([ \
    StructField("ssn",StringType(),True), \
    StructField("parents_partnership_id",IntegerType(),True), \
    StructField("first_name",StringType(),True), \
    StructField("middle_name",StringType(),True), \
    StructField("last_name",StringType(),True), \
    StructField("dob",StringType(),True), \
    StructField("sex_at_birth",StringType(),True), \
    StructField("race",StringType(),True), \
    StructField("is_hetero",BooleanType(),True), \
    StructField("is_twin",BooleanType(),True), \
    StructField("is_triplet",BooleanType(),True), \
    StructField("is_jr",BooleanType(),True), \
    StructField("event_year",IntegerType(),True)
  ])

spdf_intialize = spark.createDataFrame(data = [('',-1,'','','','','','',False,False,False,False,-1)], schema= my_schema)

spdf_intialize.write.format('delta').mode('overwrite').option('overwriteSchema',True).save(save_location)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # 2. Comment

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## SSNPool
# MAGIC
# MAGIC This was already initialized.

# COMMAND ----------

# StructType([ \
#     StructField('index', IntegerType(), True), \
#     StructField('area', StringType(), True), \
#     StructField('ssn', StringType(), True), \
#     StructField('process_end_date', IntegerType(), True), \
#     StructField('is_used', BooleanType(), True)
# ])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Births
# MAGIC
# MAGIC This was already initialized.

# COMMAND ----------

# StructType([ \
#     StructField('ssn', StringType(), True), \
#     StructField('parents_partnership_id', IntegerType(), True), \
#     StructField('first_name', StringType(), True), \
#     StructField('middle_name', StringType(), True), \
#     StructField('last_name', StringType(), True), \
#     StructField('dob', StringType(), True), \
#     StructField('sex_at_birth', StringType(), True), \
#     StructField('race', StringType(), True), \
#     StructField('is_hetero', StringType(), True), \
#     StructField('is_twin', BooleanType(), True), \
#     StructField('is_triplet', BooleanType(), True), \
#     StructField('is_jr', BooleanType(), True), \
#     StructField('event_year', IntegerType(), True)
# ])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Population
# MAGIC
# MAGIC This was already initialized.

# COMMAND ----------

# StructType([ \
#     StructField('ssn', StringType(), True), \
#     StructField('first_name', StringType(), True), \
#     StructField('middle_name', StringType(), True), \
#     StructField('last_name', StringType(), True), \
#     StructField('dob', StringType(), True), \
#     StructField('age', IntegerType(), True), \
#     StructField('sex_at_birth', StringType(), True), \
#     StructField('race', StringType(), True), \
#     StructField('zip', StringType(), True), \
#     StructField('fips', StringType(), True), \
#     StructField('house_id', LongType(), True), \
#     StructField('ever_partnered', BooleanType(), True)
# ])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## HousingLookup
# MAGIC
# MAGIC This was already initialized.

# COMMAND ----------

# StructType([ \
#     StructField('house_id', LongType(), True), \
#     StructField('building_id', IntegerType(), True), \
#     StructField('road_section_id', DoubleType(), True), \
#     StructField('bld_type', StringType(), True), \
#     StructField('num_units', DoubleType(), True), \
#     StructField('num_beds', IntegerType(), True), \
#     StructField('num_rooms', IntegerType(), True), \
#     StructField('year_built', DoubleType(), True), \
#     StructField('address_number', StringType(), True), \
#     StructField('address_street', StringType(), True), \
#     StructField('unit_type', StringType(), True), \
#     StructField('unit_number', IntegerType(), True), \
#     StructField('zip', StringType(), True), \
#     StructField('city', StringType(), True), \
#     StructField('county_name', StringType(), True), \
#     StructField('county_fips', StringType(), True), \
#     StructField('puma', StringType(), True), \
#     StructField('area_code', StringType(), True), \
#     StructField('phone_landline', StringType(), True), \
#     StructField('state', StringType(), True), \
#     StructField('is_occupied', BooleanType(), True)
# ])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## PhoneLookup
# MAGIC
# MAGIC This was already initialized.

# COMMAND ----------

# StructType([
#     StructField('phone', StringType(), True), \
#     StructField('zip', IntegerType(), True), \
#     StructField('is_used', BooleanType(), True)
# ])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## FirstNameLookup
# MAGIC
# MAGIC This was already initialized.

# COMMAND ----------

# StructType([ \
#     StructField('name', StringType(), True), \
#     StructField('race', StringType(), True), \
#     StructField('sex_at_birth', StringType(), True), \
#     StructField('year', IntegerType(), True)
# ])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## LastNameLookup
# MAGIC
# MAGIC This was already initialized.
# MAGIC Note that each last name is used once and already filtered to match 1:1 with the population in 1920.
# MAGIC For data on the probability of lastname by race, see the document:
# MAGIC **{root}\SupportingDocs\Names\03_Complete\lastname_probabilities.csv**

# COMMAND ----------

# StructType([ \
#     StructField('name', StringType(), True), \
#     StructField('race', StringType(), True)
# ])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## AdjectiveLookup
# MAGIC
# MAGIC This was already initialized.

# COMMAND ----------

# StructType([ \
#     StructField('first_letter', StringType(), True), \
#     StructField('adj', ArrayType(StringType(), True), True)
# ])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # 3. State Management 

# COMMAND ----------

# Define names/paths
dict_tables = { 'Births' : f'{synthetic_gold_exchange}/Births/Delta',
                'EmailEvents' : f'{synthetic_gold_exchange}/EmailEvents/Delta',
                'HousingEvents' : f'{synthetic_gold_exchange}/HousingEvents/Delta',
                'HousingLookup' : f'{synthetic_gold_exchange}/HousingLookup/Delta',
                'Mortality' : f'{synthetic_gold_exchange}/Mortality/Delta',
                'PartnershipEvents' : f'{synthetic_gold_exchange}/PartnershipEvents/Delta',
                'PartnershipLookup' : f'{synthetic_gold_exchange}/PartnershipLookup/Delta',
                'PhoneLookup' : f'{synthetic_gold_exchange}/PhoneLookup/Delta',
                'PhoneMobileEvents' : f'{synthetic_gold_exchange}/PhoneMobileEvents/Delta',
                'Population' : f'{synthetic_gold_exchange}/Population/Delta',
                'PreBirths' : f'{synthetic_gold_exchange}/PreBirths/Delta',
                'PrePartnerships' : f'{synthetic_gold_exchange}/PrePartnerships/Delta',
                'SSNPool' : f'{synthetic_gold_exchange}/SSNPool/Delta' }

# Create our state management table
createStateTable(synthetic_gold_stateManagement, list(dict_tables.keys()), list(dict_tables.values()))

# COMMAND ----------

logState(path=synthetic_gold_stateManagement, upcoming_year=1890)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

spdf = spark.read.format('delta').load(f'{synthetic_gold_data_path}/SyntheticGold/Delta')
spdf.display()

# COMMAND ----------


