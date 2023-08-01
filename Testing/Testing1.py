# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Testing
# MAGIC
# MAGIC The purpose of this document is to provide a space for reading in data, testing out process functions, ect.
# MAGIC
# MAGIC -------------
# MAGIC
# MAGIC Author(s): PJ Gibson
# MAGIC
# MAGIC Create Date: 2022-04-11
# MAGIC
# MAGIC Last Updated Date: 2022-04-28
# MAGIC
# MAGIC Cluster: RecordLinkage Cluster

# COMMAND ----------

# MAGIC %run "../Functions/New Process Functions"

# COMMAND ----------

# MAGIC %run "../Functions/State Management Functions"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Test the Process

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # 2. Ensure Process Running as Expected

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Define fpaths

# COMMAND ----------

# DEFINE READ FILE PATHS
#####################################################################################################################################
fpath_ExchangeBirths = f'{synthetic_gold_exchange}/Births/Delta'
fpath_ExchangePartnershipEvents = f'{synthetic_gold_exchange}/PartnershipEvents/Delta'
fpath_ExchangePopulation = f'{synthetic_gold_exchange}/Population/Delta'
fpath_ExchangePartnershipLookup = f'{synthetic_gold_exchange}/PartnershipLookup/Delta'
fpath_ExchangeHousingEvents = f'{synthetic_gold_exchange}/HousingEvents/Delta'
fpath_ExchangeHousingLookup = f'{synthetic_gold_exchange}/HousingLookup/Delta'

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Read in data

# COMMAND ----------

spdf_Births = spark.read.format('delta').load(fpath_ExchangeBirths)
spdf_HousingLookup = spark.read.format('delta').load(fpath_ExchangeHousingLookup)
spdf_Population = spark.read.format('delta').load(fpath_ExchangePopulation)
spdf_PartnershipLookup = spark.read.format('delta').load(fpath_ExchangePartnershipLookup)
spdf_PartnershipEvents = spark.read.format('delta').load(fpath_ExchangePartnershipEvents)
spdf_HousingEvents = spark.read.format('delta').load(fpath_ExchangeHousingEvents)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Partnerships

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Do IDs match up?

# COMMAND ----------

spdf_Partnership_Join = spdf_PartnershipLookup.join(spdf_PartnershipEvents, how='outer', on='partnership_id')

spdf_Partnership_Join = spdf_Partnership_Join.withColumn('ssn1_match', F.when(F.col('ssn') == F.col('partner1_ssn'), F.lit(1)).otherwise(F.lit(0)))
spdf_Partnership_Join = spdf_Partnership_Join.withColumn('ssn2_match', F.when(F.col('ssn') == F.col('partner2_ssn'), F.lit(1)).otherwise(F.lit(0)))
spdf_Partnership_Join = spdf_Partnership_Join.withColumn('ssn_matches', F.col('ssn1_match') + F.col('ssn2_match'))


print(f'{spdf_Partnership_Join.filter(F.col("partner1_ssn").isNull()).count()}: Count of unmatched records by partnership_id')
print(f'{spdf_Partnership_Join.filter(F.col("ssn_matches") == 0).count()}: Count of unmatched records by ssn')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Are people partnered multiple times?

# COMMAND ----------

print(f"{spdf_PartnershipEvents.groupby('ssn').count().agg({'count':'max'}).collect()[0][0]}: max number of partnerships per individual")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Are partnered individuals registered as partnered?

# COMMAND ----------

print(f"{spdf_PartnershipEvents.join(spdf_Population, on='ssn').filter(F.col('ever_partnered')==False).count()}: number of unregistered partnered individuals")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## HousingEvents

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Are people moving each year?  
# MAGIC
# MAGIC ### Is that number blowing up or dipping unexpectedly?

# COMMAND ----------

spdf_HousingEvents.filter('event_type > 0').groupby(['event_year','event_type']).count().orderBy('event_year','event_type').display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## HousingLookup

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### How many houses are occupied?

# COMMAND ----------

spdf_HousingLookup.filter(F.col('is_occupied') == True).count()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Population

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Who all is living together?
# MAGIC
# MAGIC Pre-1920, there shouldn't be any more than 2 people living together (partners)

# COMMAND ----------

spdf_Population.groupby('house_id').count().orderBy(F.desc('count')).filter('count == 2')\
               .join(spdf_Population, on='house_id')\
               .orderBy('house_id','age')\
               .display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Max number of people living in 1 house?
# MAGIC
# MAGIC Should be 2, as partnerships live together and nobody has started births yet.
# MAGIC
# MAGIC -------
# MAGIC
# MAGIC You'll notice that there are 2 exceptions, a house of 3 and a house of 4.
# MAGIC I have investigated this issue, and it stems from the moving process event_type = 4 where old_house_id == new_house_id.
# MAGIC I scrap them from the events when looking at same county moves, but the house is still available for random county moves.  
# MAGIC I have since patched it so that it will not happen moving forward, and am willing to accept the 2 instances where more than just 1 partnership is living together.
# MAGIC <b>Solved, and closes issue #62</b>

# COMMAND ----------

spdf_Population.groupby('house_id').count().orderBy(F.desc('count')).display()

# COMMAND ----------

spdf_Population.filter('house_id == "326417549194"').display()

# COMMAND ----------

spdf_Population.count()

# COMMAND ----------

spdf_Births.groupBy('event_year').count().orderBy('event_year').display()

# COMMAND ----------

spdf_VitalStats = spark.read.format('csv')\
                       .option('inferSchema',True)\
                       .option('header',True)\
                       .load(f'{root_synthetic_gold}/SupportingDocs/Births/03_Complete/VitalStats_byYear_byState.csv')\
                       .filter('State == "Washington"')

# COMMAND ----------

spdf_VitalStats.display()
