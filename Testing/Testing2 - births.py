# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Testing2 - births
# MAGIC
# MAGIC The purpose of this document is to provide a space where we can test out some results we might be expecting to see.
# MAGIC
# MAGIC Note 2023-07-28: This was made a long time ago and not used frequently.
# MAGIC No guarentee that the methodology for some of the checks is sound.
# MAGIC Leaving if it provides some use to somebody.
# MAGIC
# MAGIC -------------
# MAGIC
# MAGIC Author(s): PJ Gibson
# MAGIC
# MAGIC Create Date: 2022-04-11
# MAGIC
# MAGIC Last Updated Date: 2022-04-29
# MAGIC
# MAGIC Cluster: RecordLinkage Cluster

# COMMAND ----------

# MAGIC %run "../Python Global Variables"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 1. Read Data

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Load Tables

# COMMAND ----------

spdf_Births = spark.read.format('delta').load(f'{synthetic_gold_exchange}/Births/Delta')
spdf_SSNPool = spark.read.format('delta').load(f'{synthetic_gold_exchange}/SSNPool/Delta')
spdf_Population = spark.read.format('delta').load(f'{synthetic_gold_exchange}/Population/Delta')
spdf_HousingEvents = spark.read.format('delta').load(f'{synthetic_gold_exchange}/HousingEvents/Delta')
spdf_HousingLookup = spark.read.format('delta').load(f'{synthetic_gold_exchange}/HousingLookup/Delta')

spdf_PartnershipLookup = spark.read.format('delta').load(f'{synthetic_gold_exchange}/PartnershipLookup/Delta')
spdf_PartnershipEvents = spark.read.format('delta').load(f'{synthetic_gold_exchange}/PartnershipEvents/Delta')
spdf_PrePartnerships = spark.read.format('delta').load(f'{synthetic_gold_exchange}/PrePartnerships/Delta')
spdf_Mortality = spark.read.format('delta').load(f'{synthetic_gold_exchange}/Mortality/Delta')

spdf_PhoneMobileEvents = spark.read.format('delta').load(f'{synthetic_gold_exchange}/PhoneMobileEvents/Delta')
spdf_EmailEvents = spark.read.format('delta').load(f'{synthetic_gold_exchange}/EmailEvents/Delta')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 2. Tests

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### is_jr breakdown

# COMMAND ----------

ct_jr = births_from_partnerships.filter(F.col('is_jr') == True).count()

print(f'{np.round(ct_jr / ct_births_from_partnerships * 100, 3)}% : percent of births identified as a junior.')
print('7.5% : Target.  Can expect some fluxuation')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### Sexual preference breakdown

# COMMAND ----------

ct_homo = births_from_partnerships.filter(F.col('is_hetero') == False).count()

print(f'{np.round(ct_homo / ct_births_from_partnerships * 100, 3)}% : percent of births identified as homosexual.')
print('0.5% : Target.  Can expect some fluxuation')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Twin breakdown

# COMMAND ----------

ct_twins = births_from_partnerships.filter(F.col('is_twin') == True).count()


print(f'{np.round(ct_twins / ct_births_from_partnerships * 100, 3)}% : percent of births identified as twins.')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Triplet breakdown

# COMMAND ----------

ct_triplets = births_from_partnerships.filter(F.col('is_triplet') == True).count()


print(f'{np.round(ct_triplets / ct_births_from_partnerships * 100, 3)}% : percent of births identified as triplets.')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Groupby event_year

# COMMAND ----------

births_from_partnerships.groupby('event_year').count().orderBy(F.asc('event_year')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Population

# COMMAND ----------

live_newborns = spdf_Population.join(births_from_partnerships, on = 'ssn', how='inner')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Ensure right number makes it through
# MAGIC
# MAGIC \#births = \#newpopulation

# COMMAND ----------

live_newborns.count() == births_from_partnerships.join(spdf_Mortality, on='ssn', how='left_anti').count()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Normal fields

# COMMAND ----------

bool_age_young = (live_newborns.filter(F.col('age') > 18).count() == 0)
bool_never_partnered = (live_newborns.filter(F.col('ever_partnered') > True).count() == 0)
bool_unique_ssns = (live_newborns.count() == live_newborns.agg(F.countDistinct('ssn')).collect()[0][0])

print(bool_age_young)
print(bool_never_partnered)
print(bool_unique_ssns)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## PartnershipLookup

# COMMAND ----------

spdf_PartnershipLookup.filter('num_total_children > 3').orderBy(F.desc('num_total_children')).display()

# COMMAND ----------

spdf_PartnershipLookup.orderBy(F.rand(1)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## HousingEvents

# COMMAND ----------

spdf_HousingEvents.orderBy(F.rand(1)).display()

# COMMAND ----------

guide = spark.read.format('csv').option('inferSchema',True).option('header',True).load(f'{root_synthetic_gold}/SupportingDocs/Births/03_Complete/DesiredBirthsByYearByAge.csv').toPandas()

guide.set_index('Year', inplace=True)
guide['total'] = guide.sum(axis=1)

# COMMAND ----------

df_newbirths = spdf_HousingEvents.filter(F.col('event_type')==1)\
                                 .groupby('event_year').count()\
                                 .withColumnRenamed('count','total_births')\
                                 .orderBy(F.asc('event_year'))\
                                 .toPandas()

df_newbirths.set_index('event_year', inplace=True)


df_newbirths.total_births.plot(color='k')
guide.total.plot(color='red')


