# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # DirtyFunctions_Spark
# MAGIC
# MAGIC This is the official landing spot for all of the functions used to dirty up the data

# COMMAND ----------

# MAGIC %run "./Config"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # 0. Import libraries/functions

# COMMAND ----------

import string
import re
from tqdm import tqdm
import numpy as np

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # 1. General

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Transpose String

# COMMAND ----------

@udf(StringType())
def transpose(s):
  '''
  Function returns value s where 2 characters are transposed (swapped).
  
  Args:
    s, string: string to apply function to
    
  Returns:
    string
  '''
    
  if len(s) > 1:
    # Choose index of transpose start
    #### For example "Themla -> Thmela" would have transpose start index of 2 (third char in python)
    t_index1 = np.random.randint( 0, len(s)-1)

    # Choose index of tranpose end
    t_index2 = t_index1+1

    # Choose index of first character after tranpose
    t_index3 = t_index2+1

    # Return transpose version 
    return s[:t_index1]+s[t_index2]+s[t_index1]+s[t_index3:]
  
  else:
    return s

  
setattr(transpose, 'description', 'Function returns value s where 2 characters are transposed (swapped).')
setattr(transpose, 'overview', 'transpose( s: str -> str')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Delete Letter

# COMMAND ----------

@udf(StringType())
def delete_1char(s):
  '''
  Function returns value s where 1 character is deleted
  
  Args:
    s, string: string to apply function to
    
  Returns:
    string
  '''
  
  if len(s) > 1:
    
    # Choose index of delete
    #### For example "Themla -> Thela" would have transpose start index of 3 (fourth char in python)
    del_index = np.random.randint( 0, len(s)-1)

    # Return new version 
    return s[:del_index] + s[del_index+1:]

  else:
    return s

  
setattr(delete_1char, 'description', 'Function returns value s where 1 character is deleted')
setattr(delete_1char, 'overview', 'delete_1char( s: str ) -> str')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Insert Letter

# COMMAND ----------

@udf(StringType())
def insert_1letter(s):
  '''
  Function returns value s where 1 random letter character is randomly inserted into the string.
  NOTE: character will be uppercase
  
  Args:
    s, string: string to apply function to
    
  Returns:
    string
  '''
  
  if len(s) > 1:
  
    ascii_chars = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z']

    # Choose index of delete
    #### For example "Themla -> Thela" would have transpose start index of 3 (fourth char in python)
    insert_index = np.random.randint( 0, len(s)-1)

    random_char_index = np.random.randint(0, 26)

    # Return new version 
    return s[:insert_index] + ascii_chars[random_char_index] + s[insert_index:]
  
  else:
    return s
  
  
  
setattr(insert_1letter, 'description', 'Function returns value s where 1 random letter (uppercase) character is randomly inserted into the string.')
setattr(insert_1letter, 'overview', 'delete_1char( s: str ) -> str')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Insert Number

# COMMAND ----------

@udf(StringType())
def insert_1num(s):
  '''
  Function returns value s where 1 random number character is randomly inserted into the string.
  
  Args:
    s, string: string to apply function to
    
  Returns:
    string
  '''

  if len(s) > 1:
    
    ascii_chars = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']
    # Choose index of delete
    #### For example "Themla -> Thela" would have transpose start index of 3 (fourth char in python)
    insert_index = np.random.randint( 0, len(s)-1)

    random_char_index = np.random.randint(0, 10)

    # Return new version 
    return s[:insert_index] + ascii_chars[random_char_index] + s[insert_index:]
  
  else:
    return s

  
  
setattr(insert_1num, 'description', 'Function returns value s where 1 random number character is randomly inserted into the string.')
setattr(insert_1num, 'overview', 'delete_1char( s: str ) -> str')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Repeated Character

# COMMAND ----------

@udf(StringType())
def repeat_1char(s):
  '''
  Function returns value s where 1 random character is repeated once.
  
  Args:
    s, string: string to apply function to
    
  Returns:
    string
  '''
  
  if len(s) > 1:
  
    # Choose index of delete
    #### For example "Themla -> Thela" would have transpose start index of 3 (fourth char in python)
    insert_index = np.random.randint( 0, len(s)-1)

    # Return new version 
    return s[:insert_index] + s[insert_index] + s[insert_index:]
  
  else:
    return s
  

  
setattr(repeat_1char, 'description', 'Function returns value s where 1 random character is repeated once.')
setattr(repeat_1char, 'overview', 'repeat_1char( s: str ) -> str')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Leading Whitespace

# COMMAND ----------

@udf(StringType())
def leading_whitespace(s):
  '''
  Function returns value s with leading whitespace
  
  Args:
    s, string: string to apply function to
    
  Returns:
    string
  '''
  
  return ' ' + s
  
  
setattr(leading_whitespace, 'description', 'Function returns value s with leading whitespace.')
setattr(leading_whitespace, 'overview', 'leading_whitespace( s: str ) -> str')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Trailing Whitespace

# COMMAND ----------

@udf(StringType())
def trailing_whitespace(s):
  '''
  Function returns value s with trailing whitespace
  
  Args:
    s, string: string to apply function to
    
  Returns:
    string
  '''
  
  return s + ' '
  
  
setattr(trailing_whitespace, 'description', 'Function returns value s with trailing whitespace.')
setattr(trailing_whitespace, 'overview', 'trailing_whitespace( s: str ) -> str')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # 2. Specific

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## DOB

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Year = CurYear

# COMMAND ----------

@udf(StringType())
def year_equals_curyear(s):
  '''
  Function formats date yyyy-mm-dd such that the year values are 2019.  You can manually adjust the output.
  
  Args:
    s, string: string to apply function to, 
              formatted \d{4} - \d{1,2} - \d{1,2} *spaces added for emphasis ONLY
    
  Returns:
    string
  '''
  
  return re.sub('(?P<y>\d{4})-0?(?P<m>\d{1,2})-0?(?P<d>\d{1,2})', f'2019-\g<m>-\g<d>', s)

setattr(year_equals_curyear, 'description', 'Function formats date yyyy-mm-dd such that the year values are 2019.  You can manually adjust the output.')
setattr(year_equals_curyear, 'overview', 'year_equals_curyear( s: str , curyear: str) -> str')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### month <-> day

# COMMAND ----------

@udf(StringType())
def swap_month_day(s):
  '''
  Function formats date yyyy-mm-dd such that the month and day values are swapped.
  
  Args:
    s, string: string to apply function to, 
              formatted \d{4} - \d{1,2} - \d{1,2} *spaces added for emphasis ONLY
    
  Returns:
    string
  '''

  return re.sub('(?P<y>\d{4})-0?(?P<m>\d{1,2})-0?(?P<d>\d{1,2})', '\g<y>-\g<d>-\g<m>', s)

setattr(swap_month_day, 'description', 'Function formats date yyyy-mm-dd such that the month and day values are swapped.')
setattr(swap_month_day, 'overview', 'swap_month_day( s: str ) -> str')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Year as 2 digits

# COMMAND ----------

@udf(StringType())
def format_year_2digits(s):
  '''
  Function formats date yyyy-mm-dd such that the year is only 2 digits.
  
  Args:
    s, string: string to apply function to, 
              formatted \d{4} - \d{1,2} - \d{1,2} *spaces added for emphasis ONLY
    
  Returns:
    string
  '''
  
  return re.sub('(?P<y1>\d{2})(?P<y2>\d{2})-0?(?P<m>\d{1,2})-0?(?P<d>\d{1,2})', '\g<y2>-\g<m>-\g<d>', s)

setattr(format_year_2digits, 'description', 'Function formats date yyyy-mm-dd such that the year is only 2 digits.')
setattr(format_year_2digits, 'overview', 'format_year_2digits( s: str ) -> str')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Remove leading zeros

# COMMAND ----------

@udf(StringType())
def format_remove_leading_zeros(s):
  '''
  Function formats date yyyy-mm-dd -> m/d/yyyy
  
  Args:
    s, string: string to apply function to, 
              formatted \d{4} - \d{1,2} - \d{1,2} *spaces added for emphasis ONLY
    
  Returns:
    string
  '''
  
  return re.sub('(?P<y>\d{2,4})-0?(?P<m>\d{1,2})-0?(?P<d>\d{1,2})', '\g<y>-\g<m>-\g<d>', s)

setattr(format_remove_leading_zeros, 'description', 'Function formats date yyyy-mm-dd -> m/d/yyyy')
setattr(format_remove_leading_zeros, 'overview', 'format_remove_leading_zeros( s: str ) -> str')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Format to m/d/y format

# COMMAND ----------

@udf(StringType())
def format_slash_mdyyyy(s):
  '''
  Function formats date yy(yy)-(m)m-(d)d -> m/d/yy(yy)
  
  Args:
    s, string: string to apply function to, 
              formatted \d{2,4} - \d{1,2} - \d{1,2} *spaces added for emphasis ONLY
    
  Returns:
    string
  '''
  
  return re.sub('(?P<y>\d{2,4})-0?(?P<m>\d{1,2})-0?(?P<d>\d{1,2})', '\g<m>/\g<d>/\g<y>', s)

setattr(format_slash_mdyyyy, 'description', 'Function formats date yy(yy)-(m)m-(d)d -> m/d/yy(yy)')
setattr(format_slash_mdyyyy, 'overview', 'format_slash_mdyyyy( s: str ) -> str')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Phone

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Dash only

# COMMAND ----------

@udf(StringType())
def format_dash_only(s):
  '''
  Function formats phone ########## -> ###-###-####
  
  Args:
    s, string: string to apply function to, 
              formatted as 10 digits ONLY
    
  Returns:
    string
  '''
  
  return re.sub('(?P<ac>\d{3})(?P<f3>\d{3})(?P<l4>\d{4})', '\g<ac>-\g<f3>-\g<l4>', s)

setattr(format_dash_only, 'description', 'Function formats phone ########## -> ###-###-####')
setattr(format_dash_only, 'overview', 'format_dash_only( s: str ) -> str')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Parenthesis & Dash

# COMMAND ----------

@udf(StringType())
def format_parenthesis_dash(s):
  '''
  Function formats phone ########## -> (###)-###-####
  
  Args:
    s, string: string to apply function to, 
              formatted as 10 digits ONLY
    
  Returns:
    string
  '''
  
  return re.sub('(?P<ac>\d{3})(?P<f3>\d{3})(?P<l4>\d{4})', '(\g<ac>)-\g<f3>-\g<l4>', s)

setattr(format_parenthesis_dash, 'description', 'Function formats phone ########## -> (###)-###-####')
setattr(format_parenthesis_dash, 'overview', 'format_parenthesis_dash( s: str ) -> str')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Add +1 Prefix

# COMMAND ----------

@udf(StringType())
def prefix_plus1(s):
  '''
  Function formats phone so that it starts with "+1 "
  
  Args:
    s, string: string to apply function to
    
  Returns:
    string
  '''
  
  return '+1 ' + s

setattr(prefix_plus1, 'description', 'Function formats phone so that it starts with "+1 "')
setattr(prefix_plus1, 'overview', 'prefix_plus1( s: str ) -> str')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Address

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Longhand Compass Directions

# COMMAND ----------

@udf(StringType())
def longhand_compass(s):
  '''
  Function converts any cardinal directions into longhand form located at any point within the string.
  
  Args:
    s, string: string to apply function to
    
  Returns:
    string
  '''

  s = re.sub('((?<=(\s|^))N(?=[EW]?(\s|$)))', 'north', s)
  s = re.sub('((?<=(\s|^))S(?=[EW]?(\s|$)))', 'south', s)
  s = re.sub('((?<=(\s|^)((north)|(south))?)E(?=(\s|$)))', 'east', s)
  s = re.sub('((?<=(\s|^)((north)|(south))?)W(?=(\s|$)))', 'west', s)
  s = s.title()
  
  return s

setattr(longhand_compass, 'description', 'Function converts any cardinal directions into longhand form located at any point within the string.')
setattr(longhand_compass, 'overview', 'longhand_compass( s: str ) -> str')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Longhand Road abbreviations

# COMMAND ----------

@udf(StringType())
def longhand_roadtype(s):
  '''
  Function converts any road shorthands into longhand (ex: Rd -> Road ).
  Affected names include:  [St, Ave, Rd, Pl, Dr, Ct, Ln, Blvd, Hwy, Cir]
  
  
  Args:
    s, string: string to apply function to
    
  Returns:
    string
  '''

  s = re.sub('((?<=(\s|^))St(?=(\s|$)))', 'Street', s)
  s = re.sub('((?<=(\s|^))Ave(?=(\s|$)))', 'Avenue', s)
  s = re.sub('((?<=(\s|^))Rd(?=(\s|$)))', 'Road', s)
  s = re.sub('((?<=(\s|^))Pl(?=(\s|$)))', 'Place', s)
  s = re.sub('((?<=(\s|^))Dr(?=(\s|$)))', 'Drive', s)
  s = re.sub('((?<=(\s|^))Ct(?=(\s|$)))', 'Court', s)
  s = re.sub('((?<=(\s|^))Ln(?=(\s|$)))', 'Lane', s)
  s = re.sub('((?<=(\s|^))Blvd(?=(\s|$)))', 'Boulevard', s)
  s = re.sub('((?<=(\s|^))Hwy(?=(\s|$)))', 'Highway', s)
  s = re.sub('((?<=(\s|^))Cir(?=(\s|$)))', 'Circle', s)
  s = s.title()
  
  return s

setattr(longhand_roadtype, 'description', 'Function converts any road shorthands into longhand (ex: Rd -> Road ). Affected names include:  [St, Ave, Rd, Pl, Dr, Ct, Ln, Blvd, Hwy, Cir]')
setattr(longhand_roadtype, 'overview', 'longhand_compass( s: str ) -> str')

# COMMAND ----------

@udf(StringType())
def address_no_info(s):
  '''
  Return an email without any real information
  
  Args:
    s, string: string to apply function to
    
  Returns:
    string
  '''
  
  list_address_no_info = ['123 fake street', 'general delivery', 'need address', 'bad address', 'need info', 'espanol', 'na', 'need', 'no address']
  
  return str(np.random.choice(list_address_no_info))

setattr(address_no_info, 'description', 'Function returns an address containing no real information.')
setattr(address_no_info, 'overview', 'address_no_info( s: str ) -> str')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Email

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### No Provider

# COMMAND ----------

@udf(StringType())
def no_provider(s):
  '''
  Function formats email so there is no provider.  That's the @ sign and everything that follows.
  
  Args:
    s, string: string to apply function to
    
  Returns:
    string
  '''
  
  return re.sub('(?P<email_prefix>[^@]+)(?P<email_suffix>@.*)', '\g<email_prefix>', s)

setattr(no_provider, 'description', 'Function formats email so there is no provider.  That\'s the @ sign and everything that follows.')
setattr(no_provider, 'overview', 'no_provider( s: str ) -> str')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Fake Email

# COMMAND ----------

@udf(StringType())
def fake_email(s):
  '''
  Return fake email, some common ones I've come across
  
  Args:
    s, string: string to apply function to
    
  Returns:
    string
  '''
  
  list_fake_emails = [ 'g@gmail.com', 'x@gmail.com', 'noemail@noemail.com', 'a@gmail.com', '1@gmail.com', 'a@a', '999az999@yahoo.com', 'n@n',
 '1@2', 'na@na.com', 'covid19@yahoo.com', 'no@no.com', 'no@email.com', 'm@n.com', 'ar@g', 'xxx@gmail.com', 'no@yahoo.org',
  'x@g', 'noname@gmail.com', 'm@m.com', 'c19@yahoo.com', 'none@email.com', 'm@m', 'none@none.com', 'noemail@gmail.com',
   'n@n.com', 'a@msn.com', 'g@g', 'noemail@email.com', '123@gmail.com', 'refused@email.com', 'k@gmail.com', 'email@email.com',
    '123@123.com', 'no@gmail.com', 'none@none.none', 'declined@gmail.com', 'n@a', 'none@gmail.com', 'none@none', 'm@n',
     'declined@catholichealth.net', 'na@gmail.com', 'refused@yahoo.org', 'test@test.com', 'unknown@gmail.com', 'vax@gmail.com',
      'd@d.com']
  
  return str(np.random.choice(list_fake_emails))

setattr(fake_email, 'description', 'Function returns a fake email.')
setattr(fake_email, 'overview', 'fake_email( s: str ) -> str')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # 3. Combining functions

# COMMAND ----------

def apply_dirty( data , mydict , rand_seed ):
  
  # little preprocessing
  data = data.fillna('')

  # Convert dict to pandas for easy walkthrough
  pandas_boardwalk = pd.DataFrame.from_dict(mydict, orient='columns')

  # When swapping column values, you need a 3rd column to track values
  data = data.withColumn('filler', F.lit(''))

  for col_name in tqdm(pandas_boardwalk.columns) :

    # Make a new column
    data = data.withColumn(f'new__{col_name}', F.col(f'{col_name}'))

    # For each applied function...
    for func_name in pandas_boardwalk.index :

      # Determine the percent applied for the given column
      perc_value = pandas_boardwalk.loc[func_name,col_name]

      # If the percent applied is 0 or null, pass.  Move on to next function/field
      if (perc_value != perc_value):
        pass

      else:

        # Initialize flag column
        data = data.withColumn(f'flag__{col_name}__{func_name}', F.lit(False))

        # If 0% of records are affected, move on to next loop
        if (perc_value == 0):
          pass

        # If >0% of records are affected, continue to wrangle data
        else:
          
          # Apply a random value
          data = data.withColumn('random_val', F.rand(int(rand_seed)))

          # Assign flag column
          data = data.withColumn(f'flag__{col_name}__{func_name}', F.when((F.col('random_val') <= perc_value) & (F.col(f'new__{col_name}') != '') & (F.col(f'new__{col_name}') != 'None'), F.lit(True)).otherwise(F.lit(False)))
          
          ######################################################################################################################
          ######################################################################################################################

          # ...Traverse the various functions doing the following:

          if (func_name == 'null'):
            data = data.withColumn(f'new__{col_name}', F.when( F.col(f'flag__{col_name}__{func_name}') == True , F.lit('')).otherwise(F.col(f'new__{col_name}')))
            
          elif (func_name == 'nickname'):
            data = data.withColumn(f'flag__{col_name}__{func_name}', F.when( F.col('nickname')=='' , F.lit(False)).otherwise(F.col(f'flag__{col_name}__{func_name}')))
            data = data.withColumn(f'new__{col_name}', F.when( F.col(f'flag__{col_name}__{func_name}') == True , F.col('nickname')).otherwise(F.col(f'new__{col_name}')))

          elif (func_name == 'transpose'):
            data = data.withColumn(f'flag__{col_name}__{func_name}', F.when( F.length(F.col(f'new__{col_name}')) < 2 , F.lit(False)).otherwise(F.col(f'flag__{col_name}__{func_name}')))
            data = data.withColumn(f'new__{col_name}', F.when( F.col(f'flag__{col_name}__{func_name}') == True , transpose(F.col(f'new__{col_name}'))).otherwise(F.col(f'new__{col_name}')))

          elif (func_name == 'delete_1char'):
            data = data.withColumn(f'flag__{col_name}__{func_name}', F.when( F.length(F.col(f'new__{col_name}')) < 2 , F.lit(False)).otherwise(F.col(f'flag__{col_name}__{func_name}')))
            data = data.withColumn(f'new__{col_name}', F.when( F.col(f'flag__{col_name}__{func_name}') == True , delete_1char(F.col(f'new__{col_name}'))).otherwise(F.col(f'new__{col_name}')))

          elif (func_name == 'repeat_1char'):
            data = data.withColumn(f'flag__{col_name}__{func_name}', F.when( F.length(F.col(f'new__{col_name}')) < 2 , F.lit(False)).otherwise(F.col(f'flag__{col_name}__{func_name}')))
            data = data.withColumn(f'new__{col_name}', F.when( F.col(f'flag__{col_name}__{func_name}') == True , repeat_1char(F.col(f'new__{col_name}'))).otherwise(F.col(f'new__{col_name}')))

          elif (func_name == 'insert_1letter'):
            data = data.withColumn(f'flag__{col_name}__{func_name}', F.when( F.length(F.col(f'new__{col_name}')) < 2 , F.lit(False)).otherwise(F.col(f'flag__{col_name}__{func_name}')))
            data = data.withColumn(f'new__{col_name}', F.when( F.col(f'flag__{col_name}__{func_name}') == True , insert_1letter(F.col(f'new__{col_name}'))).otherwise(F.col(f'new__{col_name}')))

          elif (func_name == 'only_first_char'):
            data = data.withColumn(f'new__{col_name}', F.when( F.col(f'flag__{col_name}__{func_name}') == True , F.col(f'new__{col_name}').substr(1,1)).otherwise(F.col(f'new__{col_name}')))

          elif (func_name == 'plus__middle_name'):
            data = data.withColumn(f'flag__{col_name}__{func_name}', F.when( F.col('middle_name') == '' , F.lit(False)).otherwise(F.col(f'flag__{col_name}__{func_name}')))
            data = data.withColumn(f'new__{col_name}', F.when( F.col(f'flag__{col_name}__{func_name}') == True , F.concat(F.col(f'new__{col_name}'), F.col('middle_name'))).otherwise(F.col(f'new__{col_name}')))
            
  
          elif (func_name == 'swap__first_name'):
            data = data.withColumn(f'flag__{col_name}__{func_name}', F.when( F.col(f'new__first_name') == '' , F.lit(False)).otherwise(F.col(f'flag__{col_name}__{func_name}')))
        
            data = data.withColumn('filler', F.when( F.col(f'flag__{col_name}__{func_name}') == True ,  F.col('new__first_name')  ).otherwise(F.col('filler')))
            data = data.withColumn('new__first_name', F.when( F.col(f'flag__{col_name}__{func_name}') == True , F.col(f'new__{col_name}')).otherwise(F.col('new__first_name')))
            data = data.withColumn(f'new__{col_name}', F.when( F.col(f'flag__{col_name}__{func_name}') == True , F.col('filler')   ).otherwise(F.col(f'new__{col_name}')))
        
          elif (func_name == 'jdoe'):
            data = data.withColumn('new__first_name', F.when( (F.col(f'flag__{col_name}__{func_name}') == True) & (F.col('sex_at_birth') == 'M') , F.lit('JOHN')).otherwise(F.col('new__first_name')))
            data = data.withColumn('new__first_name', F.when( (F.col(f'flag__{col_name}__{func_name}') == True) & (F.col('sex_at_birth') == 'F') , F.lit('JANE')).otherwise(F.col('new__first_name')))
            data = data.withColumn('new__last_name', F.when( F.col(f'flag__{col_name}__{func_name}') == True , F.lit('DOE')).otherwise(F.col('new__last_name')))

          elif (func_name == 'year_equals_curyear'):
            data = data.withColumn(f'new__{col_name}', F.when( F.col(f'flag__{col_name}__{func_name}') == True , year_equals_curyear(F.col(f'new__{col_name}'))).otherwise(F.col(f'new__{col_name}')))

          elif (func_name == 'swap_month_day'):
            data = data.withColumn(f'new__{col_name}', F.when( F.col(f'flag__{col_name}__{func_name}') == True , swap_month_day(F.col(f'new__{col_name}'))).otherwise(F.col(f'new__{col_name}')))

          elif (func_name == 'format_year_2digits'):
            data = data.withColumn(f'new__{col_name}', F.when( F.col(f'flag__{col_name}__{func_name}') == True , format_year_2digits(F.col(f'new__{col_name}'))).otherwise(F.col(f'new__{col_name}')))

          elif (func_name == 'format_remove_leading_zeros'):
            data = data.withColumn(f'new__{col_name}', F.when( F.col(f'flag__{col_name}__{func_name}') == True , format_remove_leading_zeros(F.col(f'new__{col_name}'))).otherwise(F.col(f'new__{col_name}')))

          elif (func_name == 'format_slash_mdyyyy'):
            data = data.withColumn(f'new__{col_name}', F.when( F.col(f'flag__{col_name}__{func_name}') == True , format_slash_mdyyyy(F.col(f'new__{col_name}'))).otherwise(F.col(f'new__{col_name}')))

          elif (func_name == 'fake_date'):
            data = data.withColumn(f'new__{col_name}', F.when( F.col(f'flag__{col_name}__{func_name}') == True , F.lit('2000-01-01')).otherwise(F.col(f'new__{col_name}')))
            
          elif (func_name == 'dob_jan_first'):
            data = data.withColumn(f'new__{col_name}', F.when( F.col(f'flag__{col_name}__{func_name}') == True , F.col(f'new__{col_name}').substr(1,4) + F.lit('-01-01')).otherwise(F.col(f'new__{col_name}')))

          elif (func_name == 'other_O'):
            data = data.withColumn(f'new__{col_name}', F.when( F.col(f'flag__{col_name}__{func_name}') == True , F.lit('O')).otherwise(F.col(f'new__{col_name}')))

          elif (func_name == 'unknown_U'):
            data = data.withColumn(f'new__{col_name}', F.when( F.col(f'flag__{col_name}__{func_name}') == True , F.lit('U')).otherwise(F.col(f'new__{col_name}')))

          elif (func_name == 'oppositely_identify'):
            data = data.withColumn(f'new__{col_name}', F.when( (F.col(f'flag__{col_name}__{func_name}') == True) & (F.col('sex_at_birth') == 'M') , F.lit('F')).otherwise(F.col(f'new__{col_name}')))
            data = data.withColumn(f'new__{col_name}', F.when( (F.col(f'flag__{col_name}__{func_name}') == True) & (F.col('sex_at_birth') == 'F') , F.lit('M')).otherwise(F.col(f'new__{col_name}')))

          elif (func_name == 'other'):
            data = data.withColumn(f'new__{col_name}', F.when( F.col(f'flag__{col_name}__{func_name}') == True , F.lit('Other')).otherwise(F.col(f'new__{col_name}')))

          elif (func_name == 'longhand_compass'):
            data = data.withColumn(f'new__{col_name}', F.when( F.col(f'flag__{col_name}__{func_name}') == True , longhand_compass(F.col(f'new__{col_name}'))).otherwise(F.col(f'new__{col_name}')))

          elif (func_name == 'longhand_roadtype'):
            data = data.withColumn(f'new__{col_name}', F.when( F.col(f'flag__{col_name}__{func_name}') == True , longhand_roadtype(F.col(f'new__{col_name}'))).otherwise(F.col(f'new__{col_name}')))
            
          elif (func_name == 'homeless'):
            data = data.withColumn(f'new__{col_name}', F.when( F.col(f'flag__{col_name}__{func_name}') == True , F.lit('homeless')).otherwise(F.col(f'new__{col_name}')))
            
          elif (func_name == 'address_no_info'):
            data = data.withColumn(f'new__{col_name}', F.when( F.col(f'flag__{col_name}__{func_name}') == True , address_no_info(F.col(f'new__{col_name}'))).otherwise(F.col(f'new__{col_name}')))

          elif (func_name == 'no_county'):
            data = data.withColumn(f'new__{col_name}', F.when( F.col(f'flag__{col_name}__{func_name}') == True , F.regexp_replace(F.col(f'new__{col_name}'), ' County', '')).otherwise(F.col(f'new__{col_name}'))) 

          elif (func_name == 'none'):
            data = data.withColumn(f'new__{col_name}', F.when( F.col(f'flag__{col_name}__{func_name}') == True , F.lit('None')).otherwise(F.col(f'new__{col_name}')))

          elif (func_name == 'format_dash_only'):
            data = data.withColumn(f'new__{col_name}', F.when( F.col(f'flag__{col_name}__{func_name}') == True , format_dash_only(F.col(f'new__{col_name}'))).otherwise(F.col(f'new__{col_name}')))

          elif (func_name == 'format_parenthesis_dash'):
            data = data.withColumn(f'flag__{col_name}__{func_name}', F.when( F.col(f'flag__{col_name}__format_dash_only') == True , F.lit(False)).otherwise(F.col(f'flag__{col_name}__{func_name}')))
            data = data.withColumn(f'new__{col_name}', F.when( F.col(f'flag__{col_name}__{func_name}') == True , format_parenthesis_dash(F.col(f'new__{col_name}'))).otherwise(F.col(f'new__{col_name}')))

          elif (func_name == 'prefix_plus1'):
            data = data.withColumn(f'new__{col_name}', F.when( F.col(f'flag__{col_name}__{func_name}') == True , prefix_plus1(F.col(f'new__{col_name}'))).otherwise(F.col(f'new__{col_name}'))) 

          elif (func_name == 'insert_1num'):
            data = data.withColumn(f'flag__{col_name}__{func_name}', F.when( F.length(F.col(f'new__{col_name}')) < 2 , F.lit(False)).otherwise(F.col(f'flag__{col_name}__{func_name}')))
            data = data.withColumn(f'new__{col_name}', F.when( F.col(f'flag__{col_name}__{func_name}') == True , insert_1num(F.col(f'new__{col_name}'))).otherwise(F.col(f'new__{col_name}')))

          elif (func_name == 'leading_whitespace'):
            data = data.withColumn(f'new__{col_name}', F.when( F.col(f'flag__{col_name}__{func_name}') == True , leading_whitespace(F.col(f'new__{col_name}'))).otherwise(F.col(f'new__{col_name}')))

          elif (func_name == 'trailing_whitespace'):
            data = data.withColumn(f'new__{col_name}', F.when( F.col(f'flag__{col_name}__{func_name}') == True , trailing_whitespace(F.col(f'new__{col_name}'))).otherwise(F.col(f'new__{col_name}')))

          elif (func_name == 'fake_email'):
            data = data.withColumn(f'new__{col_name}', F.when( F.col(f'flag__{col_name}__{func_name}') == True , fake_email(F.col(f'new__{col_name}'))).otherwise(F.col(f'new__{col_name}')))
            
          elif (func_name == 'secondary_email'):
            data = data.withColumn(f'new__{col_name}', F.when( (F.col(f'flag__{col_name}__{func_name}') == True) & (F.col(f'flag__{col_name}__fake_email') == False), F.col('secondary_email')).otherwise(F.col(f'new__{col_name}')))
            
          elif (func_name == 'no_provider'):
            data = data.withColumn(f'new__{col_name}', F.when( F.col(f'flag__{col_name}__{func_name}') == True , no_provider(F.col(f'new__{col_name}'))).otherwise(F.col(f'new__{col_name}')))

          ###################################################################

          rand_seed += 1

  # Re-apply null values
  data = data.replace('',None)        
  
  return data

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Print Outputs

# COMMAND ----------

list_functions = [leading_whitespace, trailing_whitespace, transpose, delete_1char, insert_1letter, insert_1num, repeat_1char, year_equals_curyear, swap_month_day, format_year_2digits, format_remove_leading_zeros, format_slash_mdyyyy, longhand_compass, longhand_roadtype, address_no_info, format_dash_only, format_parenthesis_dash, prefix_plus1, no_provider, fake_email]

for function in list_functions:
  print(f'{function.overview}\n{function.description}\n')

# COMMAND ----------


