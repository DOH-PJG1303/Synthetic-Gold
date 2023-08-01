# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Create SQL database & tables
# MAGIC
# MAGIC This script serves as the location for SQL related commands that have to do with creating our initial database as well as its tables.
# MAGIC
# MAGIC -------------
# MAGIC
# MAGIC Author(s): PJ Gibson
# MAGIC
# MAGIC Create Date: 2022-05-17
# MAGIC
# MAGIC Last Updated Date: 2022-05-17

# COMMAND ----------

# MAGIC %run "../Python Global Variables"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # 1. Create Database

# COMMAND ----------

spark.sql(f"""
          
CREATE DATABASE IF NOT EXISTS Synthetic{stateExtended}
COMMENT 'This database will contain information on an entirely synthetic population based on representative Oregon/United States statistics.  The overarching goal is a gold-standard for record-linkage. This database will contain elements of complex human-nature such as twins, name changes, house moves, and partnerships (to name a few) with an underlying known truth.'
          
          """)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # 2. Create Tables

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## SSNPool

# COMMAND ----------

spark.sql(f"""
          
CREATE TABLE IF NOT EXISTS Synthetic{stateExtended}.SSNPool
USING DELTA
LOCATION '{synthetic_gold_exchange}/SSNPool/Delta'
COMMENT 'Contains information on all of the generated SSN values we have access to in this project.  These SSNs are synthetic and were not created using any PII.'          
          
          """)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Births

# COMMAND ----------

spark.sql(f"""
          
CREATE TABLE IF NOT EXISTS Synthetic{stateExtended}.Births
USING DELTA
LOCATION '{synthetic_gold_exchange}/Births/Delta'
COMMENT 'Contains birth information on each individual that passes through the simulated population.'          
   
          
          
          """)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Population

# COMMAND ----------

spark.sql(f"""
          
          
CREATE TABLE IF NOT EXISTS Synthetic{stateExtended}.Population
USING DELTA
LOCATION '{synthetic_gold_data_path}/SyntheticGold/Delta'
COMMENT 'Contains all record-linkage related fields for the population in 2022.  Note that this table is meant for time-travel.  [newest] version 101 = 2022.  [oldest] version 0 = 1921.'   
          
          
          """)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## PartnershipLookup

# COMMAND ----------

spark.sql(f"""
          
          
CREATE TABLE IF NOT EXISTS Synthetic{stateExtended}.PartnershipLookup
USING DELTA
LOCATION '{synthetic_gold_exchange}/PartnershipLookup/Delta'
COMMENT 'Contains information on all partnerships including information about the pair and the number/type of offspring.'   
          
          
          """)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## PartnershipEvents

# COMMAND ----------

spark.sql(f"""
          
          
CREATE TABLE IF NOT EXISTS Synthetic{stateExtended}.PartnershipEvents
USING DELTA
LOCATION '{synthetic_gold_exchange}/PartnershipEvents/Delta'
COMMENT 'Contains de-identified information on members of partnerships.  Good table for finding out what SSN belongs to what partnership_id and what partner number [1,2] individual has.'  
          
          
          """)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Mortality

# COMMAND ----------

spark.sql(f"""
          
          
CREATE TABLE IF NOT EXISTS Synthetic{stateExtended}.Mortality
USING DELTA
LOCATION '{synthetic_gold_exchange}/Mortality/Delta'
COMMENT 'Contains information on who died each year.'   
          
          
          """)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## HousingEvents

# COMMAND ----------

spark.sql(f"""
          
          
CREATE TABLE IF NOT EXISTS Synthetic{stateExtended}.HousingEvents
USING DELTA
LOCATION '{synthetic_gold_exchange}/HousingEvents/Delta'
COMMENT 'Contains information on all housing moves for each year based on type.'   
          
          
          """)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## HousingLookup

# COMMAND ----------

spark.sql(f"""
          
          
CREATE TABLE IF NOT EXISTS Synthetic{stateExtended}.HousingLookup
USING DELTA
LOCATION '{synthetic_gold_exchange}/HousingLookup/Delta'
COMMENT 'Contains information on all housings available to the process. The is_occupied field indicates whether or not a house is available in 2022.'   
          
          
          """)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## EmailEvents

# COMMAND ----------

spark.sql(f"""
          
          
CREATE TABLE IF NOT EXISTS Synthetic{stateExtended}.EmailEvents
USING DELTA
LOCATION '{synthetic_gold_exchange}/EmailEvents/Delta'
COMMENT 'Contains information on all emails and when they were created.   
          
          
          """)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## PhoneMobileEvents

# COMMAND ----------

spark.sql(f"""
          
          
CREATE TABLE IF NOT EXISTS Synthetic{stateExtended}.PhoneMobileEvents
USING DELTA
LOCATION '{synthetic_gold_exchange}/PhoneMobileEvents/Delta'
COMMENT 'Contains information on all once-occupied mobile phones and when they were taken by an individual.  Note that a mobile phone can be re-used if the first owner dies.'   
          
          
          """)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## PhoneMobileLookup

# COMMAND ----------

spark.sql(f"""
          
          
CREATE TABLE IF NOT EXISTS Synthetic{stateExtended}.PhoneLookup
USING DELTA
LOCATION '{synthetic_gold_exchange}/PhoneLookup/Delta'
COMMENT 'Contains information on all phones available to the process.  The is_used field indicates whether or not a phone is available in 2022.  Some phones are tied to houses that are unoccpied, but still marked as is_used=True.'   
          
          
          """)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## SyntheticGold Long

# COMMAND ----------

spark.sql(f"""
          
CREATE TABLE IF NOT EXISTS Synthetic{stateExtended}.SyntheticGoldLong
USING DELTA
LOCATION '{synthetic_gold_data_path}/LongSyntheticGold/Delta'
COMMENT 'Contains information on all demographic information for every living individual during any given year.  Called Long because it is in a "long" format.'                
          
          """)

# COMMAND ----------


