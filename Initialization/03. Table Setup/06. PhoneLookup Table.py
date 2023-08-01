# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # PhoneLookup Table
# MAGIC
# MAGIC The purpose of this file is to use the HouseLookup landline table and the PhoneLookup table in SupportingDocs to initialize the PhoneLookup table
# MAGIC
# MAGIC -------------
# MAGIC
# MAGIC Author(s): PJ Gibson
# MAGIC
# MAGIC Create Date: 2022-04-21
# MAGIC
# MAGIC Last Updated Date: 2022-04-21

# COMMAND ----------

# MAGIC %run "../../Python Global Variables"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # 1. Format / Initialize table

# COMMAND ----------

# Read in data
spdf_PhoneLookup_OLD = spark.read.format('delta').load(f"{root_synthetic_gold}/SupportingDocs/Phone/03_Complete/phone_lookup/Delta")

# Add columns
spdf_PhoneLookup_OLD = spdf_PhoneLookup_OLD.withColumn('ac', F.col('ac').cast(StringType()))
spdf_PhoneLookup_OLD = spdf_PhoneLookup_OLD.withColumn('phone_suffix', F.col('phone_suffix').cast(StringType()))
spdf_PhoneLookup_OLD = spdf_PhoneLookup_OLD.withColumn('phone', F.concat(F.col('ac'),F.col('phone_suffix')))
spdf_PhoneLookup_OLD = spdf_PhoneLookup_OLD.withColumn('is_used', F.lit(False))

# Select used columns
spdf_PhoneLookup_OLD = spdf_PhoneLookup_OLD.select(['phone','zip','is_used'])

# Save
spdf_PhoneLookup_OLD.write.format('delta').mode('overwrite').option('overwriteSchema',True).save(f'{synthetic_gold_exchange}/PhoneLookup/Delta')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # 2. Update table
# MAGIC
# MAGIC Houses each have their own unique phone_landline.
# MAGIC We should use these to update the values of the "is_used" column so other houses/people don't get assigned the same phone number.

# COMMAND ----------

spark.sql(f'''

MERGE INTO delta.`{synthetic_gold_exchange}/PhoneLookup/Delta` t
USING delta.`{synthetic_gold_exchange}/HousingLookup/Delta` s
ON t.phone = s.phone_landline
WHEN MATCHED
  THEN UPDATE SET t.is_used = True

''').display()

# COMMAND ----------


