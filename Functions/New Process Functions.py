# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Process Functions
# MAGIC 
# MAGIC The purpose of this script is to contain all of the functions to use in our process.
# MAGIC 
# MAGIC -------------
# MAGIC 
# MAGIC Author(s): PJ Gibson
# MAGIC 
# MAGIC Create Date: 2022-03-31
# MAGIC 
# MAGIC Last Updated Date: 2022-04-05

# COMMAND ----------

# MAGIC %run "../Python Global Variables"

# COMMAND ----------

# MAGIC %run "./State Management Functions"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # 1. General Processes

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### update_Age_general

# COMMAND ----------

def update_Age_general( year ):
    '''
    Update the age column in: Population table & PartnershipLookup table

    Args:
      year (int): year of current iteration

    Returns:
      *printed output*
    '''
    
    # Define fpath within function
    fpath_ExchangePopulation = f'{synthetic_gold_exchange}/Population/Delta'
    fpath_ExchangePartnershipLookup = f'{synthetic_gold_exchange}/PartnershipLookup/Delta'
    
    if year > 1890:
  
      # Update into the exchange population table
      spark.sql(f'''

      UPDATE delta.`{fpath_ExchangePopulation}` t
      SET t.age = (t.age + 1)

       ''')

      
      # Update ages of partnerships for every partnership where partner1 is living
      spark.sql(f'''

      MERGE INTO delta.`{fpath_ExchangePartnershipLookup}` t
      USING delta.`{fpath_ExchangePopulation}` s
      ON t.partner1_ssn = s.ssn
      WHEN MATCHED
        THEN UPDATE SET t.partner1_age = s.age

      ''')
    
      # Print output
      print('GENERAL: Successfully UPDATED Population, PartnershipLookup tabes - age + 1')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### update_HousingLookup_general

# COMMAND ----------

def update_HousingLookup_general():
    '''
    Update the ExchangeHousingLookup table using the ExchangePopulation table

    Args:
      No arguments

    Returns:
      *printed output*
    '''
    
    # Define fpaths
    fpath_ExchangePopulation = f'{synthetic_gold_exchange}/Population/Delta'
    fpath_ExchangeHousingLookup = f'{synthetic_gold_exchange}/HousingLookup/Delta'
    
    # Ensure that there is a 1:1 update merge statement on house_id.
    ##### Requires dropping dupes. Otherwise will error out.
    spark.read.format('delta').load(fpath_ExchangePopulation)\
              .drop_duplicates(subset=['house_id'])\
              .createOrReplaceTempView('population_unique_house_ids')
    
    
    # Initialize is occupied as false
    spark.sql(f'''
    
    UPDATE delta.`{fpath_ExchangeHousingLookup}`
    SET is_occupied = False
    
    ''')
    
    # Merge into the exchange table
    spark.sql(f'''

    MERGE INTO delta.`{fpath_ExchangeHousingLookup}` t
    USING population_unique_house_ids s
    ON t.house_id = s.house_id
    WHEN MATCHED 
        THEN UPDATE SET t.is_occupied = True
     
     ''')
    
    # Print output
    print('GENERAL: Successfully UPDATED HousingLookup table')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### update_Population_ZipFips_general

# COMMAND ----------

def update_Population_ZipFips_general():
    '''
    Update the ExchangePopulation table using the ExchangeHousingLookup table

    Args:
      No arguments

    Returns:
      *printed output*
    '''
    # Define fpaths
    fpath_ExchangePopulation = f'{synthetic_gold_exchange}/Population/Delta'
    fpath_ExchangeHousingLookup = f'{synthetic_gold_exchange}/HousingLookup/Delta'
    
    # Read in housing data
    spark.read.format('delta').load(fpath_ExchangeHousingLookup)\
              .filter(F.col('is_occupied') == True)\
              .createOrReplaceTempView('housing_data')
    
    # Merge into the exchange table
    spark.sql(f'''

    MERGE INTO delta.`{fpath_ExchangePopulation}` t
    USING housing_data s
    ON t.house_id = s.house_id
    WHEN MATCHED 
        THEN UPDATE SET t.zip = s.zip,
                        t.fips = s.county_fips
     
     ''')
    
    # Print output
    print('GENERAL: Successfully UPDATED Population table, zip & fips')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## overwrite_SyntheticGold

# COMMAND ----------

def overwrite_SyntheticGold( year ):
  '''
  Overwrite the population SyntheticGold datasets for the current year
  
  Args:
    year (int): year of current iteration

  Returns:
    *printed output*
  '''
  
  if (year >= 1920):
  
    # Read in datasets
    spdf_Population = spark.read.format('delta').load(f'{synthetic_gold_exchange}/Population/Delta')\
                           .drop('zip')

    spdf_HousingLookup = spark.read.format('delta').load(f'{synthetic_gold_exchange}/HousingLookup/Delta')

    spdf_PartnershipEvents = spark.read.format('delta').load(f'{synthetic_gold_exchange}/PartnershipEvents/Delta')\
                                  .drop('event_year')

    spdf_PhoneMobileEvents = spark.read.format('delta').load(f'{synthetic_gold_exchange}/PhoneMobileEvents/Delta')\
                                  .drop('event_year')

    spdf_EmailEvents = spark.read.format('delta').load(f'{synthetic_gold_exchange}/EmailEvents/Delta')\
                            .drop('event_year')

    # Define RL columns
    rl_cols = ['ssn','house_id','partnership_id',
               'first_name','middle_name','last_name','dob','age','sex_at_birth','race',
               'address', 'zip', 'city', 'county_name', 'county_fips', 'state',
              'phone_landline','phone_mobile','email',
              'year']

    # Overwrite Population data
    spdf_Population.join(spdf_HousingLookup, on='house_id', how='left')\
                   .join(spdf_PhoneMobileEvents, on='ssn', how='left')\
                   .join(spdf_EmailEvents, on='ssn', how='left')\
                   .join(spdf_PartnershipEvents, on='ssn', how='left')\
                   .withColumn('address',F.concat_ws(' ', F.col('address_number'), F.col('address_street'), F.col('unit_type'), F.col('unit_number')))\
                   .withColumn('year', F.lit(year))\
                   .select(rl_cols)\
                   .write.format('delta').mode('overwrite').option('overwriteSchema',True).save(f'{root_synthetic_gold}/Data/SyntheticGold/Delta')
  
    if (year == 1920):
      
      includeState( synthetic_gold_stateManagement, ['SyntheticGold'], [f'{synthetic_gold_data_path}/SyntheticGold/Delta'])
  
    print(f'PROCESS COMPLETE FOR YEAR {year}')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # 2. Partnership Process

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Spark UDFs

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### sp_will_marry

# COMMAND ----------

@udf(BooleanType(),True)
def sp_will_marry( age ):
  '''
  Returns a value of True/False indicating whether or not that person will marry this year
  Probabilities wrangled from UN/Census data gathered from the following website:
    https://population.un.org/MarriageData/Index.html#/home
  See notebook {root}/Initialization/00. Already Initialized/Partnerships/Probability Marry this Year.ipynb for how we got this array of likelyhood of getting married by each age if currently unmarried.
  
  Args:
    age (pyspark Column): age column as integer type.
  
  Returns:
    pyspark column: boolean output
  '''
  
  if (age < 18) | (age > 120):
    return bool(False)
  
  else:
    myarray = np.array([
          [18, 4.68474542e-02],
          [19, 5.84406439e-02],
          [20, 6.10876637e-02],
          [21, 6.40615739e-02],
          [22, 6.72849170e-02],
          [23, 7.06556728e-02],
          [24, 7.40520921e-02],
          [25, 7.73404419e-02],
          [26, 8.03848104e-02],
          [27, 8.30576282e-02],
          [28, 8.52493706e-02],
          [29, 8.68760842e-02],
          [30, 8.78838624e-02],
          [31, 8.82500158e-02],
          [32, 8.79812599e-02],
          [33, 8.71096385e-02],
          [34, 8.56870704e-02],
          [35, 8.37793754e-02],
          [36, 8.14604769e-02],
          [37, 7.88072658e-02],
          [38, 7.58953958e-02],
          [39, 7.27961129e-02],
          [40, 6.95740862e-02],
          [41, 6.62861335e-02],
          [42, 6.29806866e-02],
          [43, 5.96978304e-02],
          [44, 5.64697533e-02],
          [45, 5.33214656e-02],
          [46, 5.02716650e-02],
          [47, 4.73336570e-02],
          [48, 4.45162604e-02],
          [49, 4.18246526e-02],
          [50, 3.92611275e-02],
          [51, 3.68257514e-02],
          [52, 3.45169169e-02],
          [53, 3.23317964e-02],
          [54, 3.02667074e-02],
          [55, 2.83173982e-02],
          [56, 2.64792672e-02],
          [57, 2.47475280e-02],
          [58, 2.31173299e-02],
          [59, 2.15838433e-02],
          [60, 2.01423196e-02],
          [61, 1.87881297e-02],
          [62, 1.75167891e-02],
          [63, 1.63239714e-02],
          [64, 1.52055152e-02],
          [65, 1.41574257e-02],
          [66, 1.31758738e-02],
          [67, 1.22571928e-02],
          [68, 1.13978742e-02],
          [69, 1.05945631e-02],
          [70, 9.84405383e-03],
          [71, 9.14328518e-03],
          [72, 8.48933604e-03],
          [73, 7.87942135e-03],
          [74, 7.31088803e-03],
          [75, 6.78121127e-03],
          [76, 6.28799074e-03],
          [77, 5.82894696e-03],
          [78, 5.40191772e-03],
          [79, 5.00485438e-03],
          [80, 4.63581820e-03],
          [81, 4.29297668e-03],
          [82, 3.97459972e-03],
          [83, 3.67905584e-03],
          [84, 3.40480834e-03],
          [85, 3.15041138e-03],
          [86, 2.91450610e-03],
          [87, 2.69581674e-03],
          [88, 2.49314677e-03],
          [89, 2.30537505e-03],
          [90, 2.13145206e-03],
          [91, 1.97039620e-03],
          [92, 1.82129015e-03],
          [93, 1.68327737e-03],
          [94, 1.55555867e-03],
          [95, 1.43738891e-03],
          [96, 1.32807378e-03],
          [97, 1.22696679e-03],
          [98, 1.13346633e-03],
          [99, 1.04701283e-03],
          [100, 9.67086164e-04],
          [101, 8.93203040e-04],
          [102, 8.24914653e-04],
          [103, 7.61804379e-04],
          [104, 7.03485638e-04],
          [105, 6.49599857e-04],
          [106, 5.99814561e-04],
          [107, 5.53821580e-04],
          [108, 5.11335357e-04],
          [109, 4.72091366e-04],
          [110, 4.35844633e-04],
          [111, 4.02368341e-04],
          [112, 3.71452542e-04],
          [113, 3.42902938e-04],
          [114, 3.16539754e-04],
          [115, 2.92196684e-04],
          [116, 2.69719909e-04],
          [117, 2.48967181e-04],
          [118, 2.29806976e-04],
          [119, 2.12117699e-04],
          [120, 1.95786951e-04]])
    
    proba_yes = float(myarray[int(age-18),1])

#     df = pandas.DataFrame(data=myarray, columns = ['age','proba'])

#     proba_yes = float(df.query(f'age == {age}')['proba'])

    return bool(np.random.choice([True,False], p = [proba_yes, 1-proba_yes]))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### sp_choose_lname

# COMMAND ----------

@udf(StringType(),True)
def sp_choose_lname( lname1, lname2 ):
  '''
  Returns the adopted last name for a couple
  
  Args:
    lname1 (pyspark Column): lastname column as string type
    lname2 (pyspark Column): lastname column as string type
  
  Returns:
    pyspark column: string type
  '''
  
  if ('-' in lname1) | ('-' in lname2):
    array = [lname2, lname1]
    return str( np.random.choice(array, p=[0.9, 0.1]) )
  
  else:
    array = [lname2, lname1, f'{lname2}-{lname1}']
    return str( np.random.choice(array, p=[0.9, 0.09, 0.01]) )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## SubProcesses

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### insert_PrePartnership_partnerships

# COMMAND ----------

def insert_PrePartnership_partnerships( year ):
    '''
    Perform the following tasks:
      1. Select who will marry
      2. Add sexual preference, pairing number, and partner number variables
      3. Insert into PrePartnerships table

    Args:
      year (int): year of current iteration

    Returns:
      *printed output*
    '''
    
    # Define fpaths
    fpath_ExchangeBirths = f'{synthetic_gold_exchange}/Births/Delta'
    fpath_ExchangePopulation = f'{synthetic_gold_exchange}/Population/Delta'
    fpath_PrePartnerships = f'{synthetic_gold_exchange}/PrePartnerships/Delta'
    
    # Read in the population data
    spdf_pop = spark.read.format('delta').load(fpath_ExchangePopulation)    
    
    # 1. Select who will marry
    ###########################################################################
    
    # Filter out people who are already married, apply marriage function
    spdf_pop = spdf_pop.filter(F.col('ever_partnered') == False)\
                       .withColumn('will_marry', sp_will_marry(F.col('age')))\
                       .filter(F.col('will_marry') == True)\
                       .withColumn('random_bit', F.when(F.rand(1) > 0.5, F.lit(1)).otherwise(2))
    
    # 2. Add sexual preference, pairing number, and partner number variables
    ###########################################################################
    
    # Join birthing data to find out sexual preference
    spdf_births = spark.read.format('delta').load( fpath_ExchangeBirths ).select(['ssn','is_hetero'])
    
    spdf = spdf_pop.join( spdf_births, how='left', on='ssn')\
                   .withColumn('sexual_preference', F.when( ((F.col('is_hetero') == False) & (F.col('sex_at_birth') == 'F')), F.lit(1) )\
                                                   .when( ((F.col('is_hetero') == False) & (F.col('sex_at_birth') == 'M')), F.lit(2) )\
                                                   .otherwise(F.lit(3)))
    
    # Split up by sexual preference for adding window functions.  Don't work well in F.when() clauses
    spdf3 = spdf.filter( F.col('sexual_preference') == 3 )
    spdf3 = spdf3.withColumn('pairing_number',  F.row_number().over(Window.partitionBy(['sexual_preference','sex_at_birth']).orderBy('age','fips','zip')))
    spdf3 = spdf3.withColumn('partner_number', F.when(F.col('sex_at_birth') == "F" , F.lit(1)).otherwise(F.lit(2)))
    
    # Split up by sexual preference for adding window functions.  Don't work well in F.when() clauses
    spdf12 = spdf.filter( F.col('sexual_preference') != 3 )
    spdf12 = spdf12.withColumn('pairing_number',  F.row_number().over(Window.partitionBy(['sexual_preference','random_bit']).orderBy('age','fips','zip')))
    spdf12 = spdf12.withColumn('partner_number', F.col('random_bit'))
    
    # Union back up
    spdf_new = spdf3.unionByName(spdf12)
    
    spdf_new = spdf_new.withColumn('event_year', F.lit(year))
    
    spdf_new.createOrReplaceTempView('spdf_new')
    
    # 3. Add sexual preference, pairing number, and partner number variables
    ###########################################################################
    
    
    spark.sql(f'''
    
    INSERT INTO delta.`{fpath_PrePartnerships}`
    TABLE spdf_new
    
    ''')
    
    # Print output
    print('PARTNERSHIPS: Successfully INSERTED into PrePartnerships table')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### insert_PartnershipLookup_partnerships

# COMMAND ----------

def insert_PartnershipLookup_partnerships( year ):
    '''
    Perform the following tasks:
      1. Join partners
      2. Choose adopted last name for couples
      3. Assign new unique partnership_id values
      4. Insert into partnership lookup

    Args:
      year (int): year of current iteration

    Returns:
      *printed output*
    '''

    # Define fpaths
    fpath_ExchangePartnershipLookup = f'{synthetic_gold_exchange}/PartnershipLookup/Delta'
    fpath_PrePartnerships = f'{synthetic_gold_exchange}/PrePartnerships/Delta'
    
    # Read PrePartnerships
    spdf = spark.read.format('delta').load(fpath_PrePartnerships)\
                .filter(F.col('event_year') == year)
    
    # Differentiate partner1 & partner2
    spdf1 = spdf.filter(F.col('partner_number') == 1)
    spdf2 = spdf.filter(F.col('partner_number') == 2)
    
    # Create Temp views
    spdf.createOrReplaceTempView('spdf_gen_pairing')
    spdf1.createOrReplaceTempView('spdf_pair1')
    spdf2.createOrReplaceTempView('spdf_pair2')
    
    # 1. JOIN partners
    ###########################################################################
    
    # Join data
    pairs = spark.sql(f'''
    
    SELECT
      999999999 as partnership_id
      
      ,p1.sexual_preference as sexual_preference
      
      ,p1.ssn as partner1_ssn
      ,p1.race as partner1_race
      ,p1.age as partner1_age
      ,p1.last_name as partner1_last_name
      
      ,p2.ssn as partner2_ssn
      ,p2.race as partner2_race
      ,p2.first_name as partner2_first_name
      ,p2.middle_name as partner2_middle_name
      ,p2.last_name as partner2_last_name
      
      ,0 as num_singletons
      ,0 as num_twins
      ,0 as num_triplets
      ,0 as num_total_children
      
      ,{year} as event_year
    
    FROM spdf_pair1 p1
    
    INNER JOIN spdf_pair2 p2
    ON (p1.sexual_preference = p2.sexual_preference) AND (p1.pairing_number = p2.pairing_number)
    ''')
    
    # 2. Choose adopted last name for couples.  (also add has_jr column.  Difficult to enforce bool dtype in spark.sql command.)
    ###########################################################################
    
    # Assign last name
    pairs = pairs.withColumn('adopted_last_name', sp_choose_lname(F.col('partner1_last_name'),F.col('partner2_last_name')))\
                 .drop('partner1_last_name','partner2_last_name')\
                 .withColumn('has_jr', F.lit(False))

    # 3. Assign new unique partnership_id values
    ###########################################################################
    
    # Read in existing data
    partnership_lookup = spark.read.format('delta').load( fpath_ExchangePartnershipLookup )
    
    # Find maximum partnership_id value (row_num)
    max_partnership_id = int(partnership_lookup.agg({'partnership_id':'max'}).collect()[0][0])
    
    # Redefine partnership_id column
    pairs.withColumn('partnership_id', F.row_number().over(Window.partitionBy(F.lit(1)).orderBy(F.asc('partner1_ssn'))) + max_partnership_id)\
         .select(partnership_lookup.columns)\
         .createOrReplaceTempView('new_partnership_lookup')
    
    # 4. Insert into partnership lookup
    ###########################################################################
  
    # Merge into the exchange table, should be all insertions.  Otherwise, do nothing
    spark.sql(f'''

    INSERT INTO delta.`{fpath_ExchangePartnershipLookup}`
    TABLE new_partnership_lookup
 
     ''')
    
    # Print output
    print('PARTNERSHIPS: Successfully UPDATED PartnershipLookup table')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### insert_PartnershipEvents_partnerships

# COMMAND ----------

def insert_PartnershipEvents_partnerships( year ):
    '''
    Perform the following tasks:
      1. Join new pair data into PartnershipEvents format
      2. Insert all data into PartnershipEvents

    Args:
      year (int): year of current iteration

    Returns:
      *printed output*
    '''
    
    # Define fpaths
    fpath_ExchangePartnershipEvents = f'{synthetic_gold_exchange}/PartnershipEvents/Delta'
    fpath_ExchangePopulation = f'{synthetic_gold_exchange}/Population/Delta'
    fpath_ExchangePartnershipLookup = f'{synthetic_gold_exchange}/PartnershipLookup/Delta'
    
    # Create temp views
    spark.read.format('delta').load( fpath_ExchangePopulation ).createOrReplaceTempView('ExchangePopulation')

    # Load in new parternships from PartnershipLookup
    spark.read.format('delta').load( fpath_ExchangePartnershipLookup )\
              .filter(F.col('event_year') == year)\
              .createOrReplaceTempView('new_partnership_lookup')
    
    # 1. Join data into PartnershipEvents format
    ###########################################################################
    
    to_add_events1 = spark.sql(f'''
    
    SELECT 
      n.partnership_id
      , n.partner1_ssn as ssn
      , 1 as partner_number
      , n.adopted_last_name
      , pop.house_id as adopted_house_id
      , {year} as event_year
    FROM new_partnership_lookup n
    INNER JOIN ExchangePopulation pop
    ON pop.ssn = n.partner1_ssn
    
    ''')
    
    to_add_events2 = spark.sql(f'''
    
    SELECT 
      n.partnership_id
      , n.partner2_ssn as ssn
      , 2 as partner_number
      , n.adopted_last_name
      , pop.house_id as adopted_house_id
      , {year} as event_year
    FROM new_partnership_lookup n
    INNER JOIN ExchangePopulation pop
    ON pop.ssn = n.partner1_ssn
    
    ''')

    # Union up events
    to_add_events = to_add_events1.unionByName(to_add_events2)
    
    # Save temp view
    to_add_events.createOrReplaceTempView('new_partnership_events')
    
    # 2. Upsert all data into PartnershipEvents
    ###########################################################################
    
    spark.sql(f'''

    INSERT INTO delta.`{fpath_ExchangePartnershipEvents}`
    TABLE new_partnership_events
     
     ''')
    
    print('PARTNERSHIPS: Successfully INSERTED into PartnershipEvents table')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### insert_HousingEvents_partnerships

# COMMAND ----------

def insert_HousingEvents_partnerships( year ):
    '''
    Insert new partnership events into the Exchange/HousingEvents table.

    Args:
      year (int): year of current iteration

    Returns:
      *printed output*
    '''
    
    # Define fpaths
    fpath_ExchangePartnershipEvents = f'{synthetic_gold_exchange}/PartnershipEvents/Delta'
    fpath_ExchangePopulation = f'{synthetic_gold_exchange}/Population/Delta'
    fpath_ExchangeHousingEvents = f'{synthetic_gold_exchange}/HousingEvents/Delta'
    
    # Read in, create temp view
    partnership_info = spark.read.format('delta').load( fpath_ExchangePartnershipEvents )\
                            .filter(F.col('partner_number') == 2)\
                            .filter(F.col('event_year') == year)\
                            .select(['ssn','adopted_house_id','event_year'])
    
    population_info = spark.read.format('delta').load( fpath_ExchangePopulation )\
                            .select(['ssn','house_id'])
    
    new_housing_events = population_info.join(partnership_info, on='ssn', how='inner')\
                                         .withColumnRenamed('house_id', 'old_house_id')\
                                         .withColumnRenamed('adopted_house_id', 'new_house_id')\
                                         .withColumn('event_type', F.lit(3))\
                                         .select(['ssn','old_house_id','new_house_id','event_type','event_year'])\
                                         .createOrReplaceTempView('new_housing_events')
    
    # Merge into the exchange table
    spark.sql(f'''

    INSERT INTO delta.`{fpath_ExchangeHousingEvents}`
    TABLE new_housing_events
     
     ''')
    
    # Print output
    print('PARTNERSHIPS: Successfully INSERTED into HousingEvents table')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### update_Population_partnerships

# COMMAND ----------

def update_Population_partnerships( year ):
    '''
    Update the ExchangePopulation table using our newly created data

    Args:
      year (int): year of current iteration

    Returns:
      *printed output*
    '''
    
    # Define fpaths
    fpath_ExchangePartnershipEvents = f'{synthetic_gold_exchange}/PartnershipEvents/Delta'
    fpath_ExchangePopulation = f'{synthetic_gold_exchange}/Population/Delta'
    fpath_ExchangeHousingLookup = f'{synthetic_gold_exchange}/HousingLookup/Delta'
    
    spdf_HousingLookup = spark.read.format('delta').load(fpath_ExchangeHousingLookup)
    spdf_PartnershipEvents = spark.read.format('delta').load(fpath_ExchangePartnershipEvents)\
                                  .filter(F.col('event_year') == year)
    
    # Read in, create temp view
    spdf_PartnershipEvents.join( spdf_HousingLookup, spdf_PartnershipEvents.adopted_house_id == spdf_HousingLookup.house_id, how='left' )\
                          .select(['ssn','adopted_last_name','adopted_house_id','county_fips','zip'])\
                          .createOrReplaceTempView('spdf')
    
    # Merge into the exchange table
    spark.sql(f'''

    MERGE INTO delta.`{fpath_ExchangePopulation}` t
    USING spdf s
    ON t.ssn = s.ssn
    WHEN MATCHED AND s.adopted_house_id <> -1
        THEN UPDATE 
            SET t.ever_partnered = True,
                t.last_name = s.adopted_last_name,
                t.house_id = s.adopted_house_id,
                t.fips = s.county_fips,
                t.zip = s.zip
   
   WHEN MATCHED AND s.adopted_house_id == -1
       THEN UPDATE
           SET t.ever_partnered = True,
               t.last_name = s.adopted_last_name,
               t.house_id = s.adopted_house_id
     ''')
    
    # Print output
    print('PARTNERSHIPS: Successfully UPDATED Population table')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Main

# COMMAND ----------

def process_partnerships( year ):
    '''
    Process all of the new pairings for the current year

    Args:
      year (int): year of current iteration

    Returns:
      *printed output of sub-processes*
    '''
    
    # 1. Prep data for partnerships
    insert_PrePartnership_partnerships( year )
    
    # 2. Insert data into Exchange/PartnershipLookup
    insert_PartnershipLookup_partnerships( year )
    
    # 3. Insert data into Exchange/PartnershipEvents
    insert_PartnershipEvents_partnerships( year )
    
    # 4. Update Exchange/HousingEvents to reflect new housings
    insert_HousingEvents_partnerships( year )
  
    # 5. Update Exchange/Population to reflect new lastnames, ever_married = True
    update_Population_partnerships( year )
    
    # 6. Update the Exchange/HousingLookup table to reflect new living arrangements due to partnerships
    update_HousingLookup_general()
    

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # 3. Moving

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Subprocesses

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### process_18yOlds_moving

# COMMAND ----------

def process_18yOlds_moving( year ):
    '''
    Perform the following tasks:
      1. Select all 18 year olds
        - Decide who is moving within county (85%) vs outside of county (15%)
      2. Assign housings to people moving to the same county
      3. Assign housings to people moving to a random county
      4. Insert into HousingEvents
      5. Update Population
      6. Update HousingLookup
      
    Args:
      year (int): year of current iteration

    Returns:
      *printed output*
    '''
    
    # Define fpaths
    fpath_ExchangePopulation = f'{synthetic_gold_exchange}/Population/Delta'
    fpath_ExchangeHousingEvents = f'{synthetic_gold_exchange}/HousingEvents/Delta'
    fpath_ExchangeHousingLookup = f'{synthetic_gold_exchange}/HousingLookup/Delta'
    
    # Read in population data
    spdf_pop = spark.read.format('delta').load(fpath_ExchangePopulation)
    
    # 1. Select all 18 year olds
    ###########################################################################
    
    # Filter on 18 year olds, select if they will move within their county (90%) or get some random county (10%)
    spdf_pop = spdf_pop.filter(F.col('age') == 18)\
                       .withColumn('moving_loc', F.when(F.rand(year) > 0.9, F.lit('random_county')).otherwise(F.lit('same_county')))
    
    # 1a. Filter people who will move to the same county
    spdf_pop_samecounty = spdf_pop.filter(F.col('moving_loc') == 'same_county')
    
    # 1b. Filter people who will move to a random county
    spdf_pop_randcounty = spdf_pop.filter(F.col('moving_loc') == 'random_county')
    
    # 2. Assign housings to people moving to the same county
    ###########################################################################
    
    # Read in HousingLookup
    spdf_housing = spark.read.format('delta').load( fpath_ExchangeHousingLookup )
    
    # Find unoccupied housing
    spdf_available_housing = spdf_housing.filter(F.col('is_occupied') == False)\
                                         .withColumnRenamed('county_fips','fips')
    
    # Assign matching values - housing
    spdf_available_housing1 = spdf_available_housing.withColumn('match_id', F.row_number().over(Window.partitionBy('fips').orderBy(F.rand(year))))\
                                                    .select(['match_id','house_id','fips'])\
                                                    .withColumnRenamed('house_id','new_house_id')
    
    # Assign matching values - population same county
    spdf_pop_samecounty = spdf_pop_samecounty.withColumn('match_id', F.row_number().over(Window.partitionBy('fips').orderBy(F.rand(year))))\
                                             .withColumn('event_year', F.lit(year))\
                                             .select(['ssn','match_id','event_year','house_id','fips'])\
                                             .withColumnRenamed('house_id','old_house_id')
    
    # Join them
    spdf_events_samecounty = spdf_pop_samecounty.join( spdf_available_housing1, how = 'inner', on = ['match_id','fips'])\
                                                .withColumn('event_type', F.lit(2))\
                                                .select(['ssn','old_house_id','new_house_id','event_type','event_year'])
    
    # 3. Assign housings to people moving to a random county
    ###########################################################################
    
    # Find remaining available houses after last round
    spdf_available_housing2 = spdf_available_housing.join( spdf_events_samecounty, spdf_available_housing.house_id == spdf_events_samecounty.new_house_id, how='left_anti')\
    
    # Assign match_id
    spdf_available_housing2 = spdf_available_housing2.withColumn('match_id', F.row_number().over(Window.partitionBy(F.lit(1)).orderBy(F.rand(year))))\
                                                    .select(['match_id','house_id'])\
                                                    .withColumnRenamed('house_id','new_house_id')
    
    # Assign matching values - population random county
    spdf_pop_randcounty = spdf_pop_randcounty.withColumn('match_id', F.row_number().over(Window.partitionBy(F.lit(1)).orderBy(F.rand(year))))\
                                             .withColumn('event_year', F.lit(year))\
                                             .select(['ssn','match_id','event_year','house_id'])\
                                             .withColumnRenamed('house_id','old_house_id')
    
    # Join them
    spdf_events_randcounty = spdf_pop_randcounty.join( spdf_available_housing2, how = 'inner', on = 'match_id')\
                                                .withColumn('event_type', F.lit(2))\
                                                .select(['ssn','old_house_id','new_house_id','event_type','event_year'])
    
    # 4. Insert into HousingEvents
    ###########################################################################
    
    spdf_all_new_events = spdf_events_samecounty.unionByName(spdf_events_randcounty)
    spdf_all_new_events.createOrReplaceTempView('New_Housing_Events')
    
    # Merge into the exchange housing events table, should be all insertions.  Otherwise, do nothing
    spark.sql(f'''
    
    INSERT INTO delta.`{fpath_ExchangeHousingEvents}`
    TABLE New_Housing_Events
    
    ''')
    print('MOVE 18: Successfully INSERTED into HousingEvents table')
    
    # 5. Update Population
    ###########################################################################
    
    spdf_all_new_events.join(spdf_housing, spdf_all_new_events.new_house_id == spdf_housing.house_id, how='inner')\
                       .select(['ssn','new_house_id','county_fips','zip'])\
                       .createOrReplaceTempView('Updated_Population_Housing')
    
    # Merge into the population lookup table on ssn.  Updating the house_id
    spark.sql(f'''
    
    MERGE INTO delta.`{fpath_ExchangePopulation}` t
    USING Updated_Population_Housing s
    ON t.ssn = s.ssn
    WHEN MATCHED
      THEN UPDATE SET t.house_id = s.new_house_id,
                      t.fips = s.county_fips,
                      t.zip = s.zip
    
    ''')
    print('MOVE 18: Successfully UPDATED Population table - house_id')
    
    # 6. Update HousingLookup
    ###########################################################################
    update_HousingLookup_general()


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### process_general_moving
# MAGIC 
# MAGIC See the following [link](https://data.census.gov/cedsci/table?q=United%20States&t=Families%20and%20Living%20Arrangements&tid=ACSDP1Y2019.DP02) for supporting information that informs the process. 
# MAGIC 
# MAGIC I'm using the US as a general feel for how people within the state will move with some minor tweaks.  See below for the peices of information I used to inform the process:
# MAGIC 
# MAGIC ------------
# MAGIC 
# MAGIC * 13.1% live in a different house.
# MAGIC     * 7.5% (of total population) moved within same county.  <b>Round up to 7.55% to add up to 13.1%</b>.  Represents 57.6% of moves.
# MAGIC     * 5.5% (of total population) moved to a different county <b>Round up to 5.55% to add up to 13.1%</b>.  Represents 42.4% of moves.

# COMMAND ----------

def process_general_moving( year ):
    '''
    Perform the following tasks:
      1. Decide who is moving (13.1% of occupied houses)
      2a. Assign housings to people moving to the same county (85%)
      2a. Assign housings to people moving to a random county (15%)
      3. Insert all moves into HousingEvents
      4. Update Population
      5. Update HousingLookup
      
    Args:
      year (int): year of current iteration

    Returns:
      *printed output*
    '''
    
    # Define fpaths
    fpath_ExchangePopulation = f'{synthetic_gold_exchange}/Population/Delta'
    fpath_ExchangeHousingEvents = f'{synthetic_gold_exchange}/HousingEvents/Delta'
    fpath_ExchangeHousingLookup = f'{synthetic_gold_exchange}/HousingLookup/Delta'
    
    # Read in Population table
    spdf_Population = spark.read.format('delta').load(fpath_ExchangePopulation).select(['ssn','house_id'])
    
    # Read in housing lookup
    spdf_HouseLookup = spark.read.format('delta').load(fpath_ExchangeHousingLookup)
    
    # 1. Decide who is moving
    ###########################################################################
    
    spdf_HousesRelocating = spdf_HouseLookup.filter(F.col('is_occupied') == True)\
                                            .filter( F.rand(year) < 0.131 )
    
    # Split into same county moves & different county moves.  
    ###### Note that the F.rand(year) function will have a different implimentation on a different dataframe. 
    spdf_HouseRelocating_samecounty = spdf_HousesRelocating.filter( F.rand(year) < 0.5763 )
    
    spdf_HouseRelocating_randcounty = spdf_HousesRelocating.filter( F.rand(year) > 0.5763 )
    
    # 2a. Assign housings to people moving to the same county
    ###########################################################################
    
    spdf_housing_unoccupied = spdf_HouseLookup.filter(F.col('is_occupied') == False )
    
    # See available housing for same county peeps.  Applicable houses include those of people moving out, and others just vacant
    spdf_available_housing = spdf_HousesRelocating.unionByName( spdf_housing_unoccupied )
    
    # Assign matching values - housing
    spdf_available_housing1 = spdf_available_housing.withColumn('match_id', F.row_number().over(Window.partitionBy('county_fips').orderBy(F.rand(year))))\
                                                     .select(['match_id','house_id','county_fips'])\
                                                     .withColumnRenamed('house_id','new_house_id')
    
    # Assign matching values - population same county
    spdf_HouseRelocating_samecounty = spdf_HouseRelocating_samecounty.withColumn('match_id', F.row_number().over(Window.partitionBy('county_fips').orderBy(F.rand(year))))\
                                                                     .withColumn('event_year', F.lit(year))\
                                                                     .select(['match_id','event_year','house_id','county_fips'])\
                                                                     .withColumnRenamed('house_id','old_house_id')
    
    # Join the new house assignments
    spdf_events_samecounty = spdf_HouseRelocating_samecounty.join( spdf_available_housing1, how = 'inner', on = ['match_id','county_fips'])
                                                                          
    # Join the population data.  Seperated from previous command out of fear of odd join behavior                                     
    spdf_events_samecounty_PRE = spdf_events_samecounty.join( spdf_Population, spdf_events_samecounty.old_house_id == spdf_Population.house_id, how='inner')\
                                                   .withColumn('event_type', F.lit(4))\
                                                   .select(['ssn','old_house_id','new_house_id','event_type','event_year'])
                                                             
    # Cleanup very low probability situation where they move into existing house
    spdf_events_samecounty = spdf_events_samecounty_PRE.filter(F.col('old_house_id') != F.col('new_house_id'))
    
    # 2b. Assign housings to people moving to a random county
    ###########################################################################
    
    # Find out all available housings NOT just taken by last moving process
    spdf_available_housing2 = spdf_available_housing.join(spdf_events_samecounty_PRE, spdf_available_housing.house_id == spdf_events_samecounty.new_house_id, how='left_anti')
    
    # Assign matching values - housing
    spdf_available_housing2 = spdf_available_housing2.withColumn('match_id', F.row_number().over(Window.partitionBy(F.lit(1)).orderBy(F.rand(year))))\
                                                     .select(['match_id','house_id'])\
                                                     .withColumnRenamed('house_id','new_house_id')
    
    # Assign matching values - population random county
    spdf_HouseRelocating_randcounty = spdf_HouseRelocating_randcounty.withColumn('match_id', F.row_number().over(Window.partitionBy(F.lit(1)).orderBy(F.rand(year))))\
                                                                     .withColumn('event_year', F.lit(year))\
                                                                     .select(['match_id','event_year','house_id'])\
                                                                     .withColumnRenamed('house_id','old_house_id')
    
    # Join the new house assignments
    spdf_events_randcounty = spdf_HouseRelocating_randcounty.join( spdf_available_housing2, how = 'inner', on = 'match_id')
                                                                          
    # Join the population data.  Seperated from previous command out of fear of odd join behavior
    spdf_events_randcounty = spdf_events_randcounty.join( spdf_Population, spdf_events_randcounty.old_house_id == spdf_Population.house_id, how='inner')\
                                                   .withColumn('event_type', F.lit(4))\
                                                   .select(['ssn','old_house_id','new_house_id','event_type','event_year'])
    
    # Cleanup very low probability situation where they move into existing house
    spdf_events_randcounty = spdf_events_randcounty.filter(F.col('old_house_id') != F.col('new_house_id'))
    
    # 3. Insert all moves into HousingEvents
    ###########################################################################     
    
    spdf_events_all_new = spdf_events_samecounty.unionByName(spdf_events_randcounty)
    spdf_events_all_new.createOrReplaceTempView('New_HousingEvents_MovingProcess')
    
    # Merge into the exchange housing events table, should be all insertions.  Otherwise, do nothing
    spark.sql(f'''
    
    INSERT INTO delta.`{fpath_ExchangeHousingEvents}`
    TABLE New_HousingEvents_MovingProcess
    
    ''')
    print('MOVING: Successfully INSERTED into HousingEvents table')
    
    # 4. Update Population
    ###########################################################################
    
    spdf_events_all_new.join(spdf_HouseLookup, spdf_events_all_new.new_house_id == spdf_HouseLookup.house_id, how='inner')\
                       .select(['ssn','new_house_id','county_fips','zip'])\
                       .createOrReplaceTempView('New_Population_Moves')

    # Merge into the population lookup table on ssn.  Updating the house_id
    spark.sql(f'''
    
    MERGE INTO delta.`{fpath_ExchangePopulation}` t
    USING New_Population_Moves s
    ON t.ssn = s.ssn
    WHEN MATCHED
      THEN UPDATE SET t.house_id = s.new_house_id,
                      t.fips = s.county_fips,
                      t.zip = s.zip
    
    ''')
    print('MOVING: Successfully UPDATED Population table - house_id')
   
    # 5. Update HousingLookup
    ###########################################################################
    update_HousingLookup_general()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # 4. Birth Process

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Spark UDFs

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### get_BirthSchema

# COMMAND ----------

def get_BirthSchema():
    '''
    Returns the schema for the birthing table

    Returns:
      pyspark Array of StructFields()

    '''
    schema = ArrayType(StructType([
      StructField("ssn", StringType(), True),
      StructField("parents_partnership_id", IntegerType(), True),
      StructField("first_name", StringType(), True),
      StructField("middle_name", StringType(), True),
      StructField("last_name", StringType(), True),
      StructField("dob", StringType(), True),
      StructField("sex_at_birth", StringType(), True),
      StructField("race", StringType(), True),
      StructField("is_hetero", BooleanType(), True),
      StructField("is_twin", BooleanType(), True),
      StructField("is_triplet", BooleanType(), True),
      StructField("is_jr", BooleanType(), True),
      StructField("event_year", IntegerType(), True)]))

    return schema

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### sp_generate_births

# COMMAND ----------

schema = get_BirthSchema()

# COMMAND ----------

@udf(schema)
def sp_generate_births( pid, sexuality, race1, race2, fname2, mname2, lname, has_jr, event_year):
  '''
  pyspark UDF: Returns an array of arrays containing information on the new births
  
  Args:
    pid (pyspark Column): int64 type, nullable
    sexuality (pyspark Column): int64 type, nullable
    race1 (pyspark Column): string type, nullable
    race2 (pyspark Column): string type, nullable
    fname2 (pyspark Column): string type, nullable
    mname2 (pyspark Column): string type, nullable
    lname (pyspark Column): string type, nullable
    hasjr (pyspark Column): bool type, nullable
    event_year (pyspark Column): int64 type, nullable
  
  Returns:
    pyspark column: returning a list of values in between start and end
  '''
  
  # Determine how many children the couple will have
  num_kids = np.random.choice([1,2,3], p=[1-0.03-0.0009, 0.03, 0.0009])
  
  if num_kids == 1:
    is_twin = is_triplet = False
    
  elif num_kids == 2:
    is_twin = True
    is_triplet = False
    
  else:
    is_twin = False
    is_triplet = True
    
  # DOB
  start_date = date(event_year, 1, 1)
  end_date = date((event_year+1), 1, 1)
  
  time_between_dates = end_date - start_date
  days_between_dates = time_between_dates.days
  
  days_after_jan1 = int(np.random.choice(np.arange(0,days_between_dates)))
  time_delta = pandas.Timedelta(days_after_jan1, unit = 'D')
  pre_dob = time_delta + pandas.Timestamp(f'{event_year}-01-01 00:00:00')
  dob =  str(pre_dob)[:10]
  
  
  output_list = []
  any_jr_trigger = False
  
  for i in np.arange(0,num_kids):
    
    # Initialize columns
    ssn = ''
    parents_partnership_id = int(pid)
    first_name = ''
    middle_name = ''
    last_name = str(lname)
    # dob already defined
    sex_at_birth = str(np.random.choice(['F','M']))
    race = str(np.random.choice([race1, race2]))
    is_hetero = bool(np.random.choice([True,False] , p=[0.995, 0.005]))
    # is_twin already defined
    # is_triplet already defined
    # is_jr described below
    year = int(event_year)
    
    # Determine if they are a twin
    myseed = np.random.random()
    if (myseed < 0.15) & (sex_at_birth == 'M') & (any_jr_trigger == False) & (sexuality != 1) & (has_jr == False):
      is_jr = True
      first_name = str(fname2)
      middle_name = str(mname2)
      any_jr_trigger = True
    else:
      is_jr = False
      
    output_list.append( (ssn, parents_partnership_id, first_name, middle_name, last_name, dob, sex_at_birth, race, is_hetero, is_twin, is_triplet, is_jr, year) )
  
  return output_list

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### sp_newname

# COMMAND ----------

@udf(StringType())
def sp_newname( taken, a, b, c, d, e ):
  '''
  pyspark UDF: Returns an array of arrays containing information on the new births
  
  Args:
    taken (pyspark Column): array type, nullable
    a (pyspark Column): string type, nullable.  Represents name to choose
    b (pyspark Column): string type, nullable.  Represents name to choose
    c (pyspark Column): string type, nullable.  Represents name to choose
    d (pyspark Column): string type, nullable.  Represents name to choose
    e (pyspark Column): string type, nullable.  Represents name to choose

  Returns:
    pyspark column: string type 
  '''
  output = ''
  for name in [a,b,c,d,e]:
    if name not in taken:
      output = name
      break
  if len(output) == 0:
    silly_names = ['OPTIMUS','BATMAN','MOWGLI','JERMAJESTY','NUTELLA','TITAN','BURGER','HOTPIE','LITTLEFINGER','GREYWORM','DAENERYS','CERSEI','VARYS','TYWIN']
    output = np.random.choice(silly_names)
  
  return str(output)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## SubProcesses

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### insert_PreBirths_births

# COMMAND ----------

def insert_PreBirths_births( year ):
  '''
  Inserts new records into PreBirths delta table.
  
  Args:
    year (int): year of current iteration
  
  Returns:
    *printed output*
  '''
  ####### Find eligible partnerships
  
  # Read in Population data
  spdf_Population = spark.read.format('delta').load(f'{synthetic_gold_exchange}/Population/Delta')\
                                .select(['ssn'])\
                                .withColumnRenamed('ssn','partner1_ssn')
  
  # Read in PartnershipLookup data, filter to 18-44 age inclusive, ensure that partner1 (birth-dependent-parent) is living == ssn in the population.
  spdf_eligible_Partnerships = spark.read.format('delta').load(f'{synthetic_gold_exchange}/PartnershipLookup/Delta')\
                                     .filter((F.col('partner1_age') > 17) & (F.col('partner1_age') < 45))\
                                     .join( spdf_Population, on = 'partner1_ssn', how='inner')

  # Find out how many couples have same Partner1_age.  Also add F.rand() column
  spdf_eligible_Partnerships = spdf_eligible_Partnerships.withColumn('age_grouped_count', F.count('*').over(Window.partitionBy('partner1_age').orderBy(F.lit(1))))\
                                                         .withColumn('rand_col', F.rand(year))

  ##### Read/format our guide that lists number births by year by age we desire
  guide = spark.read.format('csv').option('inferSchema',True).option('header',True).load(f'{root_synthetic_gold}/SupportingDocs/Births/03_Complete/DesiredBirthsByYearByAge.csv').toPandas()
  guide = guide.query(f'Year == {year}').drop('Year',axis=1).transpose().reset_index()
  guide.columns = ['age','DesiredCountBirths']
  spdf_guide = spark.createDataFrame(guide)

  ##### Join data to our guide
  spdf_eligible_Partnerships = spdf_eligible_Partnerships.join(spdf_guide, spdf_guide.age == spdf_eligible_Partnerships.partner1_age, how='inner')

  # Determine likelyhood of partnership giving birth
  spdf_eligible_Partnerships = spdf_eligible_Partnerships.withColumn('prob_birth', F.col('DesiredCountBirths') / F.col('age_grouped_count'))\
                                                         .withColumn('will_birth', F.when( F.col('rand_col') < F.col('prob_birth') , F.lit(True) ).otherwise(F.lit(False)))

  # Filter down to people who WILL give birth
  spdf_eligible_Partnerships = spdf_eligible_Partnerships.filter(F.col('will_birth') == True)

  # Main function for giving birth
  spdf_eligible_Partnerships = spdf_eligible_Partnerships.withColumn('birth_data', sp_generate_births(F.col("partnership_id"),\
                                                                                                                F.col("sexual_preference"),\
                                                                                                                F.col("partner1_race"),\
                                                                                                                F.col("partner2_race"),\
                                                                                                                F.col("partner2_first_name"),\
                                                                                                                F.col("partner2_middle_name"),\
                                                                                                                F.col("adopted_last_name"),\
                                                                                                                F.col("has_jr"), \
                                                                                                                F.lit(year)))
  
  # Select the main column of interest, explode it, split out
  spdf_newbirths = spdf_eligible_Partnerships.select("birth_data")\
                                             .withColumn('exploded', F.explode('birth_data'))\
                                             .select(F.col("exploded.*"))

  # Create temp view
  spdf_newbirths.createOrReplaceTempView('spdf_newbirths')

  # Insert into PreBirths
  spark.sql(f'''

      INSERT INTO delta.`{synthetic_gold_exchange}/PreBirths/Delta`
      TABLE spdf_newbirths

      ''')

  # Print output
  print('BIRTHS: Successfully INSERTED into PreBirths table')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### insert_Births_births

# COMMAND ----------

def insert_Births_births( year ):
  '''
  Assigns names, ssn to PreBirths data.  Then inserts into Exchange/Births
  
  Args:
    year (int): year of current iteration
  
  Returns:
    *printed output*
  '''
  
  # Read in new birth information - pre name/ssn assignment
  spdf_newbirths = spark.read.format('delta').load(f'{synthetic_gold_exchange}/PreBirths/Delta').filter(f'event_year == {year}')
  
  # Read in partnership lookup info
  spdf_PartnershipLookup = spark.read.format('delta').load(f'{synthetic_gold_exchange}/PartnershipLookup/Delta')\
                                                     .select('partnership_id','partner2_first_name')\
                                                     .withColumnRenamed('partnership_id', 'parents_partnership_id')
  
  # Read in birth info, find out what first names have been used in a family
  spdf_Births = spark.read.format('delta').load(f'{synthetic_gold_exchange}/Births/Delta')\
                                          .select('parents_partnership_id','first_name')
  
  spdf_combined_info =  spdf_PartnershipLookup.join( spdf_Births, on='parents_partnership_id', how='left')\
                                              .fillna('', subset=['first_name'])\
                                              .drop('partner2_first_name')
  
  # Incorporate partner2_first_name into list of things we don't want to use as fname.  Can cause issue with is_jr process
  spdf_combined_info = spdf_combined_info.unionByName( spdf_PartnershipLookup.withColumnRenamed('partner2_first_name','first_name') )
  
  # Get one neat dataframe of occupied fnames (arrayType) for each partnership_id
  agg_names = spdf_combined_info.groupby('parents_partnership_id').agg(F.collect_list('first_name').alias('taken_first_names'))

  ######################
  # Naming Process
  ######################
  
  # Filter out named people and unamed people.  NAMED are juniors, UNAMED is everyone else.
  spdf_newbirths_NAMED = spdf_newbirths.filter(F.col('first_name') != '')
  
  spdf_newbirths_UNNAMED = spdf_newbirths.filter(F.col('first_name') == '')\
                                         .drop('first_name')\
                                         .drop('middle_name')\
                                         .withColumn('match_number', F.row_number().over(Window.partitionBy(['race','sex_at_birth']).orderBy(F.rand(1))))

  if year <= 2021:
    # Read in name data
    spdf_names = spark.read.format('delta').load(f'{synthetic_gold_exchange}/FirstNameLookup/Delta')\
                      .filter(f'year == {year}')
  
  else:
    spdf_names = spark.read.format('delta').load(f'{synthetic_gold_exchange}/FirstNameLookup/Delta')\
                      .filter('year == 2021')

  # Create first name dataframe (shuffled by random(year))
  spdf_fnames1 = spdf_names.withColumn('match_number', F.row_number().over(Window.partitionBy(['race','sex_at_birth']).orderBy(F.rand(year))))\
                          .withColumnRenamed('name','first_name1')
  
  spdf_fnames2 = spdf_names.withColumn('match_number', F.row_number().over(Window.partitionBy(['race','sex_at_birth']).orderBy(F.rand(year*3))))\
                          .withColumnRenamed('name','first_name2')
  
  spdf_fnames3 = spdf_names.withColumn('match_number', F.row_number().over(Window.partitionBy(['race','sex_at_birth']).orderBy(F.rand(year*4))))\
                          .withColumnRenamed('name','first_name3')
  
  spdf_fnames4 = spdf_names.withColumn('match_number', F.row_number().over(Window.partitionBy(['race','sex_at_birth']).orderBy(F.rand(year*5))))\
                          .withColumnRenamed('name','first_name4')
  
  spdf_fnames5 = spdf_names.withColumn('match_number', F.row_number().over(Window.partitionBy(['race','sex_at_birth']).orderBy(F.rand(year*6))))\
                          .withColumnRenamed('name','first_name5')

  # Create middle name dataframe (shuffled by random(year x 2))
  spdf_mnames = spdf_names.withColumn('match_number', F.row_number().over(Window.partitionBy(['race','sex_at_birth']).orderBy(F.rand(year*2))))\
                          .withColumnRenamed('name','middle_name')

  # Assign names via join
  spdf_newbirths_NOWNAMED = spdf_newbirths_UNNAMED.join( spdf_fnames1 , on = ['race','sex_at_birth','match_number'], how='inner')\
                                                  .join( spdf_fnames2 , on = ['race','sex_at_birth','match_number'], how='inner')\
                                                  .join( spdf_fnames3 , on = ['race','sex_at_birth','match_number'], how='inner')\
                                                  .join( spdf_fnames4 , on = ['race','sex_at_birth','match_number'], how='inner')\
                                                  .join( spdf_fnames5 , on = ['race','sex_at_birth','match_number'], how='inner')\
                                                  .join( spdf_mnames , on = ['race','sex_at_birth','match_number'], how='inner')
  
  # Select the real first name to be chosen
  spdf_newbirths_NOWNAMED = spdf_newbirths_NOWNAMED.join( agg_names, on = 'parents_partnership_id' , how='inner')\
                                                    .withColumn('first_name', sp_newname(F.col('taken_first_names'), F.col('first_name1'), F.col('first_name2'), F.col('first_name3'), F.col('first_name4'), F.col('first_name5')))
  
  # Minor cleanup in case that twins/triplets get same first names.  Make middle name their first name.  Still non-zero chance of issues, but I'm okay with it.
  spdf_newbirths_NOWNAMED = spdf_newbirths_NOWNAMED.withColumn('fixmecol', F.row_number().over(Window.partitionBy('parents_partnership_id','first_name').orderBy(F.asc_nulls_last('middle_name'))))\
                                                   .withColumn('first_name', F.when(F.col('fixmecol') > 1, F.col('middle_name')).otherwise(F.col('first_name')))\
                                                   .select( spdf_newbirths_NAMED.columns )

  # Join back everything with names now
  spdf_newbirths_all_named = spdf_newbirths_NOWNAMED.unionByName( spdf_newbirths_NAMED )\
                                                    .drop('ssn')\
                                                    .withColumn('ssn_match_id', F.row_number().over(Window.partitionBy(F.lit(1)).orderBy(F.asc('dob'))))

  ######################
  # SSN Process
  ######################
  
  # Read in SSN Pool data, only look at unused SSNs
  spdf_SSNPool = spark.read.format('delta').load(f'{synthetic_gold_exchange}/SSNPool/Delta')\
                      .filter(F.col('is_used') == False)

  #### Assign SSNPool row_number

  # If year before 2011, SSN assignment is incrimental
  if (year < 2011):

    # Filter records down
    spdf_SSNPool = spdf_SSNPool.filter(F.col('process_end_date') == 2011)

    # Create a row_number column
    spdf_SSNPool = spdf_SSNPool.withColumn('ssn_match_id', F.row_number().over(Window.partitionBy(F.lit(1)).orderBy(F.asc('index'))))

  # ... otherwise, SSN assignment is random
  else:

    spdf_SSNPool = spdf_SSNPool.withColumn('ssn_match_id', F.row_number().over(Window.partitionBy(F.lit(1)).orderBy(F.rand(year))))

  # Create a temp view
  spdf_SSNPool = spdf_SSNPool.select(['ssn','ssn_match_id'])

  # Assign SSN
  spdf_newbirths_final = spdf_newbirths_all_named.join(spdf_SSNPool, on = 'ssn_match_id', how='inner')\
                                                 .select( spdf_newbirths_NAMED.columns )

  ######################
  # Cleanup / Prep
  ######################
  
  # Remove middle name for people who randomly got same first,middle name
  spdf_newbirths_final = spdf_newbirths_final.withColumn('middle_name', F.when(F.col('first_name') == F.col('middle_name'), F.lit(None)).otherwise(F.col('middle_name')))
  
  # Create temp view
  spdf_newbirths_final.createOrReplaceTempView('spdf_newbirths_final')

  ######################
  # INSERT INTO BIRTHS
  ######################

  spark.sql(f'''

      INSERT INTO delta.`{synthetic_gold_exchange}/Births/Delta`
      TABLE spdf_newbirths_final

      ''')

  print('BIRTHS: Successfully INSERTED into Births table')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### update_SSNPool_births

# COMMAND ----------

def update_SSNPool_births( year ):
  '''
  Using births from this year, updates the SSNPool 'is_used' column
  
  Args:
    year (int): year of current iteration
  
  Returns:
    *printed output*
  '''
  spark.read.format('delta').load(f'{synthetic_gold_exchange}/Births/Delta').filter(F.col('event_year')==year).createOrReplaceTempView('new_births')

  spark.sql(f'''

      MERGE INTO delta.`{synthetic_gold_exchange}/SSNPool/Delta` t
      USING new_births s
      ON t.ssn = s.ssn
      WHEN MATCHED
        THEN UPDATE SET t.is_used = True

      ''')

  print('BIRTHS: Successfully UPDATED SSNPool table')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### insert_Population_births

# COMMAND ----------

def insert_Population_births( year ):
  '''
  Using births from this year (and other tables), insert new people into the population
  
  Args:
    year (int): year of current iteration
  
  Returns:
    *printed output*
  '''
  
  # Read in Birth data from this year
  spdf_NewBirths = spark.read.format('delta').load(f'{synthetic_gold_exchange}/Births/Delta')\
                        .filter(F.col('event_year')==year)

  # Read in PartnershipLookup data
  spdf_PartnershipLookup = spark.read.format('delta').load(f'{synthetic_gold_exchange}/PartnershipLookup/Delta')\
                                .select(['partnership_id','partner1_ssn'])

  # Read in population data
  spdf_Population = spark.read.format('delta').load(f'{synthetic_gold_exchange}/Population/Delta')\
                                .select(['house_id','ssn'])\
                                .withColumnRenamed('ssn','partner1_ssn')

  # Read in housing data
  spdf_HousingLookup = spark.read.format('delta').load(f'{synthetic_gold_exchange}/HousingLookup/Delta')\
                            .select(['house_id','county_fips','zip'])\
                            .withColumnRenamed('county_fips','fips')

  # Identify the columns of the population table
  population_cols = ['ssn','first_name','middle_name','last_name','dob','age','sex_at_birth','race','zip','fips','house_id','ever_partnered']

  # Join all data, select used columns, create temp view for insert statement
  spdf_NewBirths.join( spdf_PartnershipLookup , on = spdf_PartnershipLookup.partnership_id == spdf_NewBirths.parents_partnership_id, how='inner')\
                .join( spdf_Population , on = 'partner1_ssn', how='inner')\
                .join( spdf_HousingLookup, on = 'house_id', how='inner' )\
                .withColumn('ever_partnered', F.lit(False))\
                .withColumn('age', F.lit(0))\
                .select(population_cols)\
                .createOrReplaceTempView('spdf_NewPopulation')

  # Insert into the population
  spark.sql(f'''

      INSERT INTO delta.`{synthetic_gold_exchange}/Population/Delta`
      TABLE spdf_NewPopulation

      ''')

  # Return printed statement
  print('BIRTHS: Successfully INSERTED into Population table')
  

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### insert_HousingEvents_births

# COMMAND ----------

def insert_HousingEvents_births( year ):
  '''
  Using new Population members (age == 0), insert housing events into table. (event_type == 1)
  
  Args:
    year (int): year of current iteration
  
  Returns:
    *printed output*
  '''
  
  # Read in population data for new births
  spdf_Population_newbirths = spark.read.format('delta').load(f'{synthetic_gold_exchange}/Population/Delta')\
                                   .filter(F.col('age')==0)

  # Minor wrangling
  spdf_Population_newbirths.withColumnRenamed('house_id', 'new_house_id')\
                           .withColumn('old_house_id', F.lit(None))\
                           .withColumn('event_type', F.lit(1))\
                           .withColumn('event_year', F.lit(year))\
                           .select(['ssn','old_house_id','new_house_id','event_type','event_year'])\
                           .createOrReplaceTempView('new_HousingEvents_births')

  # Insert into the housing events log
  spark.sql(f'''

      INSERT INTO delta.`{synthetic_gold_exchange}/HousingEvents/Delta`
      TABLE new_HousingEvents_births

      ''')

  # Print output
  print('BIRTHS: Successfully INSERTED into HousingEvents table')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### update_PartnershipLookup_births

# COMMAND ----------

def update_PartnershipLookup_births( year ):
  '''
  Using new birthing data, update the number children/has_jr fields.
  
  Args:
    year (int): year of current iteration
  
  Returns:
    *printed output*
  '''
  
  # Read in new birthing data
  spdf_newbirths = spark.read.format('delta').load(f'{synthetic_gold_exchange}/Births/Delta')\
                        .filter(F.col('event_year')==year)

  # Wrangle birthing data to find number triplets, twins, singletons, overall children from past year.  
  #### ORDER BY F.desc('is_jr') is very important.  Allows us to update has_jr field in clever way using OR statement.
  spdf_newbirths.select(['parents_partnership_id', 'is_twin', 'is_triplet', 'is_jr'])\
                      .withColumn('num_new_children', F.count('*').over(Window.partitionBy('parents_partnership_id').orderBy(F.lit(1))))\
                      .withColumn('num_singletons', F.when(F.col('num_new_children') == 1, F.lit(1)).otherwise(F.lit(0)))\
                      .withColumn('num_twins', F.when(F.col('num_new_children') == 2, F.lit(2)).otherwise(F.lit(0)))\
                      .withColumn('num_triplets', F.when(F.col('num_new_children') == 3, F.lit(3)).otherwise(F.lit(0)))\
                      .withColumn('row_num', F.row_number().over(Window.partitionBy('parents_partnership_id').orderBy(F.desc('is_jr'))))\
                      .filter(F.col('row_num') == 1)\
                      .createOrReplaceTempView('NewBirth_info_PartnerLookup')

  # Update PartnershipLookup to indicate new birth aggregated data
  spark.sql(f'''

      MERGE INTO delta.`{synthetic_gold_exchange}/PartnershipLookup/Delta` t
      USING NewBirth_info_PartnerLookup s
      ON s.parents_partnership_id = t.partnership_id
      WHEN MATCHED
        THEN UPDATE SET
          t.num_singletons = (t.num_singletons + s.num_singletons) , 
          t.num_twins = (t.num_twins + s.num_twins) ,
          t.num_triplets = (t.num_triplets + s.num_triplets) , 
          t.num_total_children = (t.num_total_children + s.num_new_children) ,
          t.has_jr = (t.has_jr OR s.is_jr)
          
      ''')

  print('BIRTHS: Successfully UPDATED PartnershipLookup table')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Main

# COMMAND ----------

def process_births( year ):
    '''
    Process all of the new births for the current year, updating & inserting records from various tables as needed

    Args:
      year (int): year of current iteration

    Returns:
      *printed outputs of sub-processes*
    '''
    
    if (year > 1920):

      # 1. Find eligible partnerships, do initial processing of births.  Insert into PreBirths table
      insert_PreBirths_births( year )

      # 2. Assign names to non-jrs, SSN to all records. Insert into Birth table
      insert_Births_births( year )

      # 3. Update "is_used" column of SSNPool table using information on new births.
      update_SSNPool_births( year )

      # 4. Find housing information for new births. Insert new births into the Population table.
      insert_Population_births( year )

      # 5. Insert newborns' event records into HousingEvents table.
      insert_HousingEvents_births( year )

      # 6. Update PartnershipLookup table fields to reflect number newborns per couple, presence of jr. in family.
      update_PartnershipLookup_births( year )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # 5. Mortality

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Spark UDFs

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### sp_will_die

# COMMAND ----------

@udf(BooleanType(),True)
def sp_will_die( age, sex_at_birth ):
  '''
  Returns a value of True/False indicating whether or not that person will die this year
     original data found here -> https://www.ssa.gov/oact/STATS/table4c6_2019_TR2021.html
  
  Args:
    age (pyspark Column): age column as int type.
    sex_at_birth (pyspark Column): sex_at_birth column as string type.
  
  Returns:
    pyspark column: boolean output
  '''
  
  # Note that this data is copy & pasted from the last cell in the notebook "{root}/Initialization/00. Already Initialized/Mortality/Scrape Actuarial Life Table"
  #### pulled from https://www.ssa.gov/oact/STATS/table4c6_2019_TR2021.html
  np_mortality = np.array([
       [0.00000e+00, 6.08100e-03, 5.04600e-03],
       [1.00000e+00, 4.25000e-04, 3.49000e-04],
       [2.00000e+00, 2.60000e-04, 2.12000e-04],
       [3.00000e+00, 1.94000e-04, 1.66000e-04],
       [4.00000e+00, 1.54000e-04, 1.37000e-04],
       [5.00000e+00, 1.42000e-04, 1.22000e-04],
       [6.00000e+00, 1.35000e-04, 1.11000e-04],
       [7.00000e+00, 1.27000e-04, 1.03000e-04],
       [8.00000e+00, 1.17000e-04, 9.80000e-05],
       [9.00000e+00, 1.04000e-04, 9.50000e-05],
       [1.00000e+01, 9.70000e-05, 9.60000e-05],
       [1.10000e+01, 1.06000e-04, 1.02000e-04],
       [1.20000e+01, 1.45000e-04, 1.16000e-04],
       [1.30000e+01, 2.20000e-04, 1.39000e-04],
       [1.40000e+01, 3.24000e-04, 1.70000e-04],
       [1.50000e+01, 4.37000e-04, 2.04000e-04],
       [1.60000e+01, 5.52000e-04, 2.40000e-04],
       [1.70000e+01, 6.76000e-04, 2.78000e-04],
       [1.80000e+01, 8.06000e-04, 3.19000e-04],
       [1.90000e+01, 9.39000e-04, 3.60000e-04],
       [2.00000e+01, 1.07900e-03, 4.05000e-04],
       [2.10000e+01, 1.21500e-03, 4.51000e-04],
       [2.20000e+01, 1.32700e-03, 4.91000e-04],
       [2.30000e+01, 1.40600e-03, 5.23000e-04],
       [2.40000e+01, 1.46100e-03, 5.50000e-04],
       [2.50000e+01, 1.50800e-03, 5.75000e-04],
       [2.60000e+01, 1.55900e-03, 6.05000e-04],
       [2.70000e+01, 1.61200e-03, 6.42000e-04],
       [2.80000e+01, 1.67100e-03, 6.91000e-04],
       [2.90000e+01, 1.73400e-03, 7.49000e-04],
       [3.00000e+01, 1.79800e-03, 8.11000e-04],
       [3.10000e+01, 1.86000e-03, 8.72000e-04],
       [3.20000e+01, 1.92600e-03, 9.33000e-04],
       [3.30000e+01, 1.99400e-03, 9.90000e-04],
       [3.40000e+01, 2.06700e-03, 1.04600e-03],
       [3.50000e+01, 2.14700e-03, 1.10700e-03],
       [3.60000e+01, 2.23300e-03, 1.17200e-03],
       [3.70000e+01, 2.31800e-03, 1.23600e-03],
       [3.80000e+01, 2.39900e-03, 1.29600e-03],
       [3.90000e+01, 2.48300e-03, 1.35600e-03],
       [4.00000e+01, 2.58100e-03, 1.42300e-03],
       [4.10000e+01, 2.69700e-03, 1.50200e-03],
       [4.20000e+01, 2.82800e-03, 1.59600e-03],
       [4.30000e+01, 2.97600e-03, 1.70900e-03],
       [4.40000e+01, 3.14500e-03, 1.84000e-03],
       [4.50000e+01, 3.33900e-03, 1.98800e-03],
       [4.60000e+01, 3.56600e-03, 2.15200e-03],
       [4.70000e+01, 3.83100e-03, 2.33200e-03],
       [4.80000e+01, 4.14200e-03, 2.52800e-03],
       [4.90000e+01, 4.49800e-03, 2.74400e-03],
       [5.00000e+01, 4.88800e-03, 2.98000e-03],
       [5.10000e+01, 5.31900e-03, 3.24000e-03],
       [5.20000e+01, 5.80800e-03, 3.52900e-03],
       [5.30000e+01, 6.36000e-03, 3.85200e-03],
       [5.40000e+01, 6.97000e-03, 4.20700e-03],
       [5.50000e+01, 7.62700e-03, 4.59000e-03],
       [5.60000e+01, 8.32000e-03, 4.99600e-03],
       [5.70000e+01, 9.04700e-03, 5.42500e-03],
       [5.80000e+01, 9.80300e-03, 5.87400e-03],
       [5.90000e+01, 1.05910e-02, 6.34600e-03],
       [6.00000e+01, 1.14470e-02, 6.88000e-03],
       [6.10000e+01, 1.23520e-02, 7.45400e-03],
       [6.20000e+01, 1.32480e-02, 8.00600e-03],
       [6.30000e+01, 1.41170e-02, 8.51500e-03],
       [6.40000e+01, 1.49950e-02, 9.02500e-03],
       [6.50000e+01, 1.59870e-02, 9.61000e-03],
       [6.60000e+01, 1.71070e-02, 1.03200e-02],
       [6.70000e+01, 1.82800e-02, 1.11580e-02],
       [6.80000e+01, 1.95000e-02, 1.21480e-02],
       [6.90000e+01, 2.08290e-02, 1.33010e-02],
       [7.00000e+01, 2.23640e-02, 1.46620e-02],
       [7.10000e+01, 2.41690e-02, 1.62100e-02],
       [7.20000e+01, 2.62490e-02, 1.78920e-02],
       [7.30000e+01, 2.86420e-02, 1.97010e-02],
       [7.40000e+01, 3.13800e-02, 2.17000e-02],
       [7.50000e+01, 3.45930e-02, 2.40640e-02],
       [7.60000e+01, 3.82350e-02, 2.68140e-02],
       [7.70000e+01, 4.21590e-02, 2.98370e-02],
       [7.80000e+01, 4.63360e-02, 3.31320e-02],
       [7.90000e+01, 5.09170e-02, 3.68100e-02],
       [8.00000e+01, 5.62050e-02, 4.11020e-02],
       [8.10000e+01, 6.23270e-02, 4.60800e-02],
       [8.20000e+01, 6.91900e-02, 5.16580e-02],
       [8.30000e+01, 7.68440e-02, 5.78680e-02],
       [8.40000e+01, 8.54070e-02, 6.48290e-02],
       [8.50000e+01, 9.50100e-02, 7.26900e-02],
       [8.60000e+01, 1.05770e-01, 8.15780e-02],
       [8.70000e+01, 1.17771e-01, 9.15870e-02],
       [8.80000e+01, 1.31063e-01, 1.02774e-01],
       [8.90000e+01, 1.45666e-01, 1.15160e-01],
       [9.00000e+01, 1.61582e-01, 1.28749e-01],
       [9.10000e+01, 1.78797e-01, 1.43532e-01],
       [9.20000e+01, 1.97287e-01, 1.59491e-01],
       [9.30000e+01, 2.17013e-01, 1.76600e-01],
       [9.40000e+01, 2.37930e-01, 1.94825e-01],
       [9.50000e+01, 2.58655e-01, 2.13248e-01],
       [9.60000e+01, 2.78786e-01, 2.31570e-01],
       [9.70000e+01, 2.97897e-01, 2.49466e-01],
       [9.80000e+01, 3.15556e-01, 2.66589e-01],
       [9.90000e+01, 3.31333e-01, 2.82585e-01],
       [1.00000e+02, 3.47900e-01, 2.99540e-01],
       [1.01000e+02, 3.65295e-01, 3.17512e-01],
       [1.02000e+02, 3.83560e-01, 3.36563e-01],
       [1.03000e+02, 4.02738e-01, 3.56756e-01],
       [1.04000e+02, 4.22875e-01, 3.78162e-01],
       [1.05000e+02, 4.44018e-01, 4.00852e-01],
       [1.06000e+02, 4.66219e-01, 4.24903e-01],
       [1.07000e+02, 4.89530e-01, 4.50397e-01],
       [1.08000e+02, 5.14007e-01, 4.77421e-01],
       [1.09000e+02, 5.39707e-01, 5.06066e-01],
       [1.10000e+02, 5.66692e-01, 5.36430e-01],
       [1.11000e+02, 5.95027e-01, 5.68616e-01],
       [1.12000e+02, 6.24778e-01, 6.02733e-01],
       [1.13000e+02, 6.56017e-01, 6.38896e-01],
       [1.14000e+02, 6.88818e-01, 6.77230e-01],
       [1.15000e+02, 7.23259e-01, 7.17864e-01],
       [1.16000e+02, 7.59422e-01, 7.59422e-01],
       [1.17000e+02, 7.97393e-01, 7.97393e-01],
       [1.18000e+02, 8.37263e-01, 8.37263e-01],
       [1.19000e+02, 8.79126e-01, 8.79126e-01],
       [1.20000e+02, 1.00000e+00, 1.00000e+00]])

#   df_mortality = pandas.DataFrame(np_mortality)
#   df_mortality.columns = ['Age','MaleProb','FemaleProb']
#   df_mortality['Age'] = df_mortality['Age'].astype(int)
  
  if sex_at_birth == 'M':
    proba = np_mortality[int(age),1]
#     proba = df_mortality.query(f'Age == {age}')['MaleProb'].iloc[0]
  
  else:
    proba = np_mortality[int(age),2]
#     proba = df_mortality.query(f'Age == {age}')['FemaleProb'].iloc[0]
  
  return bool(np.random.choice([True,False], p = [proba, 1-proba]))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Subprocesses

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### insert_Mortality_mortality

# COMMAND ----------

def insert_Mortality_mortality( year ):
  '''
  Determine who will die this year & insert into mortality table

  Args:
    year (int): year of current iteration

  Returns:
    *printed output*
  '''
  
  # Define fpath(s)
  fpath_ExchangePopulation = f'{synthetic_gold_exchange}/Population/Delta'
  fpath_ExchangeMortality = f'{synthetic_gold_exchange}/Mortality/Delta'

  # Read in Population data
  spdf_Population = spark.read.format('delta').load(fpath_ExchangePopulation)
  
  # Assign column indicating whether or not you'll die
  spdf_Population.withColumn('die_this_year', sp_will_die(F.col('age'),F.col('sex_at_birth')))\
                 .filter(F.col('die_this_year') == True)\
                 .withColumn('event_year', F.lit(year))\
                 .select('ssn','age','event_year')\
                 .withColumnRenamed('age','age_at_death')\
                 .createOrReplaceTempView('new_deaths')
  
  # Insert into mortality
  spark.sql(f'''
  
  INSERT INTO delta.`{fpath_ExchangeMortality}`
  TABLE new_deaths
  
  ''')
  
  print('MORTALITY: Successfully INSERTED into Mortality table')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### update_PhoneLookup_mortality

# COMMAND ----------

def update_PhoneLookup_mortality( year ):
  '''
  Use newly inserted Mortality records to update PhoneLookup for mobile phones

  Args:
    year (int): year of current iteration

  Returns:
    *printed output*
  '''
  
  # Define fpath(s)
  fpath_ExchangeMortality = f'{synthetic_gold_exchange}/Mortality/Delta'
  fpath_ExchangePhoneLookup = f'{synthetic_gold_exchange}/PhoneLookup/Delta'
  fpath_ExchangePhoneMobileEvents = f'{synthetic_gold_exchange}/PhoneMobileEvents/Delta'
  
  if (year > 1990):
    
    # Read in Population data
    spdf_NewDeaths = spark.read.format('delta').load(fpath_ExchangeMortality)\
                          .filter(F.col('event_year') == year)

    # Read in Mobile Events
    spdf_PhoneMobileEvents = spark.read.format('delta').load(fpath_ExchangePhoneMobileEvents)\
                                  .withColumnRenamed('phone_mobile','phone')

    # Read in Phone Lookup
    spdf_PhoneLookup = spark.read.format('delta').load(fpath_ExchangePhoneLookup)


    # Assign column indicating whether or not you'll die
    spdf_NewDeaths.join( spdf_PhoneMobileEvents, on='ssn', how='inner')\
                  .join( spdf_PhoneLookup, on='phone', how='inner')\
                  .select(['phone'])\
                  .createOrReplaceTempView('unused_phones')

    # Update PhoneLookup
    spark.sql(f'''

    MERGE INTO delta.`{fpath_ExchangePhoneLookup}` t
    USING unused_phones s
    ON t.phone = s.phone
    WHEN MATCHED
      THEN UPDATE SET t.is_used = False

    ''')

  print('MORTALITY: Successfully UPDATED into PhoneLookup table')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### update_Population_mortality

# COMMAND ----------

def update_Population_mortality( year ):
  '''
  Use newly inserted Mortality records to update Population table

  Args:
    year (int): year of current iteration

  Returns:
    *printed output*
  '''
  
  # Define fpath(s)
  fpath_ExchangeMortality = f'{synthetic_gold_exchange}/Mortality/Delta'
  fpath_ExchangePopulation = f'{synthetic_gold_exchange}/Population/Delta'

  # Update Population
  spark.sql(f'''

  MERGE INTO delta.`{fpath_ExchangePopulation}` t
  USING delta.`{fpath_ExchangeMortality}` s
  ON t.ssn = s.ssn
  WHEN MATCHED
    THEN DELETE

  ''')

  print('MORTALITY: Successfully UPDATED into Population table')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Main

# COMMAND ----------

def process_mortality( year ):
    '''
    Process all of the deaths for the current year

    Args:
      year (int): year of current iteration

    Returns:
      *printed outputs of sub-processes*
    '''
    
    if (year > 1920):
    
      # 1. Find out who died this year, insert into mortality table
      insert_Mortality_mortality( year )

      # 2. Update PhoneLookup table to indicate phone "is_used" == False for all newly dead individuals.
      update_PhoneLookup_mortality( year )

      # 3. Remove newly dead individuals from population
      update_Population_mortality( year )

      # 4. Update the Exchange/HousingLookup table to reflect newly open houses "is_occupied" == False
      update_HousingLookup_general()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # 6. Emails

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Spark UDFs

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### sp_generate_emails

# COMMAND ----------

@udf(ArrayType(StringType(),True))
def sp_generate_emails( fname, mname, lname, adjs, year ):
  '''
  Returns 10 emails for an individual based on fname,mname,lname
  
  that is an alliteration with the first name
  
  Args:
    fname (pyspark Column): firstname column as string type
    mname (pyspark Column): middlename column as string type
    lname (pyspark Column): lastname column as string type
    adj (pyspark Column): list of adjectives as array type
  
  Returns:
    pyspark column: string type
  '''
  
  adjs = list(adjs)
  
  if mname is None:
    m_initial = ''
  
  # Otherwise, treat normally
  else:
    m_initial = mname[0]
  
  # Make first and last initials
  f_initial = fname[0]
  l_initial = lname[0]
  
  str_first = str(fname)
  str_last = str(lname)
  
  # Account for hotmail>outlook change
  if (year < 2012):
    list_email_extensions = ['GMAIL.COM','YAHOO.COM','HOTMAIL.COM','AOL.COM']
    list_email_probabilities = [.30,.30,.30,.10]
    
  elif (year == 2012):
    list_email_extensions = ['GMAIL.COM','YAHOO.COM','HOTMAIL.COM', 'OUTLOOK.COM','AOL.COM']
    list_email_probabilities = [.30,.30,.15,.15,.10]
    
  else:
    list_email_extensions = ['GMAIL.COM','YAHOO.COM','OUTLOOK.COM','AOL.COM']
    list_email_probabilities = [.30,.30,.30,.10]
  
  ####################################################################################################
  # Inididual preference defining
  #
  # Users will have favorite numbers, adjectives, email providers that we define here
  ####################################################################################################
  
  # All 10 output emails will share the same extension.  
  # ... While people can have multiple, we're gonna say everyone is perfectly loyal to their email provider.
  preferred_extension = np.random.choice(list_email_extensions, p=list_email_probabilities)
  
  # find out preferred numbers :)
  num1 = str(np.random.choice(np.arange(0,10)))
  num2 = str(np.random.choice(np.arange(0,100))).zfill(2)
  num3 = str(np.random.choice(np.arange(0,1000))).zfill(3)
  num4 = str(np.random.choice(np.arange(0,10000))).zfill(4)
  
  # For se
  preferred_break_char = str(np.random.choice(['.','-']))
  
  #################################
  # Silly addresses - adjectives
  #################################

  # If fname 1st letter is doesn't have many adjectives, still give some options
  if len(adjs) < 20:
    for adjective in ['AWESOME','THEREAL','THE','COOL-CAT','CATS-PAJAMAS','GROOVY','CHOICE','FETCH','SUPERCALIFRAGILISTICEXPIALIDOCIOUS']:
      adjs.append(adjective)
  
  # Select one of the random adjectives
  preferred_adjective1 = np.random.choice(adjs)
  preferred_adjective2 = np.random.choice(adjs)

  ##################################################
  # EMAIL OPTIONS!
  ##################################################

  # Professional type, NO special characters, NO numbers
  email1 = f"{str_first}{str_last}@{preferred_extension}"
  email2 = f"{f_initial}{str_last}@{preferred_extension}"
  email3 = f"{f_initial}{m_initial}{str_last}@{preferred_extension}"
  
  # Professional type, WITH special characters ['.','-'] , NO numbers
  email4 = f"{str_first}{preferred_break_char}{str_last}@{preferred_extension}"
  email5 = f"{f_initial}{preferred_break_char}{str_last}@{preferred_extension}"
  email6 = f"{f_initial}{m_initial}{preferred_break_char}{str_last}@{preferred_extension}"
  
  # Professional type, NO special characters, WITH numbers
  email7 = f"{str_first}{str_last}{num1}@{preferred_extension}"
  email8 = f"{str_first}{str_last}{num2}@{preferred_extension}"
  email9 = f"{str_first}{str_last}{num3}@{preferred_extension}"
  email10 = f"{num1}{str_first}{str_last}@{preferred_extension}"
  
  email11 = f"{f_initial}{str_last}{num1}@{preferred_extension}"
  email12 = f"{f_initial}{str_last}{num2}@{preferred_extension}"
  email13 = f"{f_initial}{str_last}{num3}@{preferred_extension}"
  email14 = f"{num1}{f_initial}{str_last}@{preferred_extension}"
  
  email15 = f"{f_initial}{m_initial}{str_last}{num1}@{preferred_extension}"
  email16 = f"{f_initial}{m_initial}{str_last}{num2}@{preferred_extension}"
  email17 = f"{f_initial}{m_initial}{str_last}{num3}@{preferred_extension}"
  email18 = f"{num1}{f_initial}{m_initial}{str_last}@{preferred_extension}"
  
  # Silly type.  Adjective + Fname
  email19 = f"{preferred_adjective1}-{str_first}@{preferred_extension}"
  email20 = f"{preferred_adjective2}-{str_first}@{preferred_extension}"

  # Return list of all emails
  return list([email1, email2, email3, email4, email5, email6, email7, email8, email9, email10,
              email11, email12, email13, email14, email15, email16, email17, email18, email19, email20])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Main

# COMMAND ----------

def process_emails( year ):
  '''
  Process all of the new emails for the current year

  Args:
    year (int): year of current iteration

  Returns:
    *printed outputs of sub-processes*
  '''
  
  if (year >= 1996):

    # Define relevant fpaths
    fpath_ExchangePopulation = f'{synthetic_gold_exchange}/Population/Delta'
    fpath_AdjectiveLookup = f'{synthetic_gold_exchange}/AdjectiveLookup/Delta'
    fpath_EmailEvents = f'{synthetic_gold_exchange}/EmailEvents/Delta'
    fpath_ProbaByYear = f'{synthetic_gold_supporting}/Email/03_Complete/ProbaEmailThisYear.csv'

    # Read in data
    spdf_AdjectiveLookup =  spark.read.format('delta').load(fpath_AdjectiveLookup)
    spdf_EmailEvents = spark.read.format('delta').load(fpath_EmailEvents)
    pddf_ProbaEmail = spark.read.format('csv').option('inferSchema',True).option('header',True).load(fpath_ProbaByYear).toPandas()
    spdf_Population =  spark.read.format('delta').load(fpath_ExchangePopulation)\
                            .filter(F.col('age') > 14)\
                            .select(['ssn','first_name','middle_name','last_name'])

    # Define eligible population, hasn't had an email yet
    spdf_eligible = spdf_Population.join(spdf_EmailEvents, on='ssn', how='left_anti')\
                                   .withColumn('event_year', F.lit(year))

    # Find counts of population total/eligible for an email
    perc_eligible_recieving_email = pddf_ProbaEmail.query(f'Year == {year}')['perc'].iloc[0]
    ct_population = spdf_Population.count()
    ct_eligible = spdf_eligible.count()

    # using math, determine how many people should get new emails
    ct_desired_email_newbies = int((ct_eligible * perc_eligible_recieving_email))

    if (ct_desired_email_newbies > 0):

      # Determine who will get an email this year, then find related adjectives for the person based on first letter of first name
      spdf_chosen = spdf_eligible.withColumn('row_num', F.row_number().over(Window.partitionBy(F.lit(1)).orderBy(F.rand(year))))\
                                 .filter(F.col('row_num') <= ct_desired_email_newbies)\
                                 .drop('row_num')\
                                 .withColumn('first_letter', F.col('first_name').substr(1,1))\
                                 .join(spdf_AdjectiveLookup, on='first_letter', how='left')

      ######## Run email generation, drop unused column, dedupe, ensure no emails overlap with existing ones, dedupe again, select 2 emails per person
      spdf_chosen = spdf_chosen.withColumn('email', F.explode(sp_generate_emails( F.col('first_name'), F.col('middle_name'), F.col('last_name'), F.col('adj'), F.col('event_year') )))\
                               .drop('adj')\
                               .drop_duplicates(subset=['email'])\
                               .join(spdf_EmailEvents, on='email', how='left_anti')\
                               .withColumn('rownum', F.row_number().over(Window.partitionBy('ssn').orderBy(F.rand(1))))\
                               .filter(F.col('rownum')<=2)

      # For each person, get one row with a secondary column "emails" (ListType) containing the 2 chosen emails for the person
      email_grouped =  spdf_chosen.groupBy('ssn').agg(F.collect_set('email').alias('emails'))\
                                  .withColumn('email', F.col('emails')[0])\
                                  .withColumn('secondary_email',F.col('emails')[1])\
                                  .drop('emails')

      # Join back together and select columns in proper order
      spdf_chosen.select('ssn','event_year')\
                 .drop_duplicates()\
                 .join(email_grouped, on='ssn', how='inner')\
                 .select(['ssn','email','secondary_email','event_year'])\
                 .createOrReplaceTempView('new_emails')

      ########################################################################
      # Insert into email events

      spark.sql(f'''

      INSERT INTO delta.`{fpath_EmailEvents}`
      TABLE new_emails

      ''')
      
      print('EMAILS: Successfully INSERTED into EmailEvents table')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # 7. Mobile Phones

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## SubProcesses

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### inset_PhoneMobileEvents_mobilephones

# COMMAND ----------

def inset_PhoneMobileEvents_mobilephones( year ):
  '''
  Inserts new mobile phone assignments into the PhoneMobileEvents table

  Args:
    year (int): year of current iteration

  Returns:
    *printed outputs of sub-processes*
  '''

  # Define relevant fpaths
  fpath_ExchangePopulation = f'{synthetic_gold_exchange}/Population/Delta'
  fpath_PhoneMobileEvents = f'{synthetic_gold_exchange}/PhoneMobileEvents/Delta'
  fpath_PhoneLookup = f'{synthetic_gold_exchange}/PhoneLookup/Delta'
  fpath_ProbaByYear = f'{synthetic_gold_supporting}/Phone/03_Complete/mobile_phone_proba_by_year.csv'

  # Read in data
  pddf_ProbaPhone = spark.read.format('csv').option('inferSchema',True).option('header',True).load(fpath_ProbaByYear).toPandas()

  spdf_PhoneMobileEvents = spark.read.format('delta').load(fpath_PhoneMobileEvents)

  # Only assign mobile phones to people older than 14
  spdf_Population = spark.read.format('delta').load(fpath_ExchangePopulation)\
                          .filter(F.col('age') > 14)\
                          .select('ssn')

  spdf_AvailablePhones = spark.read.format('delta').load(fpath_PhoneLookup)\
                              .filter(F.col('is_used')==False)\
                              .withColumn('row_num', F.row_number().over(Window.partitionBy(F.lit(1)).orderBy(F.rand(year))))


  spdf_eligible = spdf_Population.join( spdf_PhoneMobileEvents, on='ssn', how='left_anti')


  # Find counts of population total/eligible for an email
  perc_having_phone = pddf_ProbaPhone.query(f'year == {year}')['perc'].iloc[0]
  ct_population = spdf_Population.count()
  ct_eligible = spdf_eligible.count()

  # using math, determine how many people should get new emails
  ct_desired_phone_newbies = int((perc_having_phone - (1-(ct_eligible / ct_population))) * ct_population)

  if (ct_desired_phone_newbies > 0):

    # Determine who will get an email this year, then find related adjectives for the person based on first letter of first name
    spdf_chosen = spdf_eligible.withColumn('row_num', F.row_number().over(Window.partitionBy(F.lit(1)).orderBy(F.rand(year))))\
                               .filter(F.col('row_num') <= ct_desired_phone_newbies)\
                               .join( spdf_AvailablePhones, on='row_num', how='inner')\
                               .withColumn('event_year', F.lit(year))\
                               .withColumnRenamed('phone','phone_mobile')\
                               .select(['ssn','phone_mobile','event_year'])\
                               .createOrReplaceTempView('new_phones')

    ########################################################################
    # Insert into PhoneMobileEvents

    spark.sql(f'''

    INSERT INTO delta.`{fpath_PhoneMobileEvents}`
    TABLE new_phones

    ''')

    print('PHONES: Successfully INSERTED into PhoneMobileEvents table')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### update_PhoneLookup_mobilephones

# COMMAND ----------

def update_PhoneLookup_mobilephones( year ):
  '''
  Inserts new mobile phone assignments into the PhoneMobileEvents table

  Args:
    year (int): year of current iteration

  Returns:
    *printed outputs of sub-processes*
  '''

  # Define relevant fpaths
  fpath_PhoneMobileEvents = f'{synthetic_gold_exchange}/PhoneMobileEvents/Delta'
  fpath_PhoneLookup = f'{synthetic_gold_exchange}/PhoneLookup/Delta'

  spark.sql(f'''

  SELECT *
  FROM delta.`{fpath_PhoneMobileEvents}`
  WHERE event_year == {year}

  ''').createOrReplaceTempView('new_phone_events')

  ########################################################################
  # Insert into PhoneMobileEvents

  spark.sql(f'''

  MERGE INTO delta.`{fpath_PhoneLookup}` t
  USING new_phone_events s
  ON t.phone = s.phone_mobile
  WHEN MATCHED
    THEN UPDATE SET t.is_used = True

  ''')

  print('PHONES: Successfully UPDATED into PhoneLookup table')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Main

# COMMAND ----------

def process_mobilephones( year ):
  '''
  Process all of the new mobile phone assignments for the current year

  Args:
    year (int): year of current iteration

  Returns:
    *printed outputs of sub-processes*
  '''
  
  if (year >= 1980):
  
    # 1. Determine who gets new mobile phones, assign them, and insert into PhoneMobileEvents table
    inset_PhoneMobileEvents_mobilephones( year )

    # 2. Update the PhoneLookup to represent newly taken phones
    update_PhoneLookup_mobilephones( year )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # 8. Extras
# MAGIC 
# MAGIC These functions serve to support the process but aren't directly tied to the simulation.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## file_exists

# COMMAND ----------

def file_exists(path):
  '''
  Returns true if the specified path exists.  Otherwise returns false.

  Args:
    path (str): user defined path to check for

  Returns:
    bool
  '''
  try:
    dbutils.fs.ls(path)
    return True
  except Exception as e:
    if 'java.io.FileNotFoundException' in str(e):
      return False
    else:
      raise

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # 9. Overall process

# COMMAND ----------

def run_simulation( year ):
  '''
  Runs all processes related to the simulation after initial setup has been completed.  
  Should be ran year-after-year from 1889 - 2022 inclusive

  Args:
    year (int): year of current iteration

  Returns:
    *printed output*
  '''
  
  # Update ages
  update_Age_general( year )
  
  if year >= 1980:
    process_mobilephones( year )
  
  if year >= 1996:
    process_emails( year )
  
  # Move 18 year olds
  process_18yOlds_moving( year )

  # Create partnerships
  process_partnerships( year )
  
  # Move people
  process_general_moving( year )
  
  # New babies :)
  process_births( year )
  
  # Deaths :(
  process_mortality( year )
  
  # SyntheticGold
  overwrite_SyntheticGold( year )
  
  # LogState
  logState(path=synthetic_gold_stateManagement, upcoming_year=year+1)
