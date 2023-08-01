# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## AdjectiveLookup
# MAGIC
# MAGIC The purpose of this script is to initialize a adjectivelookup table so that we can assign silly emails to people.
# MAGIC
# MAGIC -------------
# MAGIC
# MAGIC Author(s): PJ Gibson
# MAGIC
# MAGIC Create Date: 2022-05-03
# MAGIC
# MAGIC Last Updated Date: 2022-05-03
# MAGIC
# MAGIC Cluster: RecordLinkage Cluster

# COMMAND ----------

# MAGIC %pip install nltk

# COMMAND ----------

# MAGIC %run "../../Python Global Variables"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # 1. Import Libraries

# COMMAND ----------

# Import libraries, dependencies, and sub-libraries
import nltk
nltk.download('wordnet')
nltk.download('omw-1.4')
from nltk.corpus import wordnet as wn

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # 2. Configure Adjectives

# COMMAND ----------

# Get adjectives
all_adjectives = [word for synset in wn.all_synsets('a') for word in synset.lemma_names()]

# Get their frequency counts
freqs = nltk.FreqDist(w for w in all_adjectives)

# Take top 1000 adjectives (by frequency)
df_adjectives = pd.DataFrame.from_dict(dict(freqs), orient='index').sort_values(0, ascending=False)

# Format dataframe
df_adjectives.reset_index(inplace=True)
df_adjectives.rename(columns={0:'count', 'index':'adj'}, inplace=True)
df_adjectives['adj'] = df_adjectives['adj'].str.upper()

# Assign new column indicating first letter of the adjective
df_adjectives['first_letter'] = df_adjectives['adj'].str.slice(stop=1).str.upper()

# Make sure to remove non A-Z adjectives (ex: 4th)
df_adjectives = df_adjectives[df_adjectives['first_letter'].str.contains('[A-Z]',regex=True)]

# Remove roman numerals
df_adjectives = df_adjectives[~df_adjectives['adj'].str.contains('^[XILVC]+$', regex=True)]

# Convert to spark quickly for temp operation
spdf_temp = df_adjectives.to_spark()

# Get the only take a maximum of 100 adjectives per first letter
spdf_temp = spdf_temp.withColumn('row_num', F.row_number().over(Window.partitionBy('first_letter').orderBy(F.desc('count'),F.rand(1))))\
                     .filter(F.col('row_num') < 101)\
                     .drop('row_num')

# convert back to pyspark pandas
df_adjectives = pd.DataFrame(spdf_temp)


# Filter to columns we care about
df_adjectives = df_adjectives[['first_letter','adj']]

# Convert to base pandas, groupby firstletter, aggregate adjectives to list, format
pddf_adjectives = df_adjectives.to_spark().toPandas().groupby(['first_letter']).agg({'adj': lambda x: x.tolist()}).reset_index()

# Convert to pyspark
spdf_adjectives = spark.createDataFrame(pddf_adjectives)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # 3. Write out

# COMMAND ----------

spdf_adjectives.write.format('delta')\
               .mode('overwrite')\
               .option('overwriteSchema',True)\
               .save(f'{synthetic_gold_exchange}/AdjectiveLookup/Delta')

# COMMAND ----------


