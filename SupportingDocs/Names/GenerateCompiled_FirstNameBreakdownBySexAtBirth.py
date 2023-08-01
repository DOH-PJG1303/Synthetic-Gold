# -*- coding: utf-8 -*-
"""
Created on Thu Oct  7 15:27:15 2021

@author: PJ Gibson
"""

#%% Import Libraries

import numpy as np
import pandas as pd
import os
import regex as re
import pandasql as ps

#%% Read & Concat data.

# Check out the folder that contains the list of .txt files from the SSA with sex-breakdown information on first names
dir_contents = os.listdir('01_Raw/BreakdownBySexAtBirth')

# Initialize our output dataframe
output_df = pd.DataFrame(columns=['Year','FirstName','SexAtBirth','Count'])

# Loop through all files in the folder
for file in dir_contents:
    
    # If they are a txt file, dive in
    if file.endswith('.txt'):
        
        # Using regex parse out the year associated with the file
        current_file_year = re.findall('\d{4}', file)[0]
        
        # Read in the file using pandas
        current_file_contents = pd.read_csv('01_Raw/BreakdownBySexAtBirth/' + file, header=None)
        
        # Rename columns
        current_file_contents.columns = ['FirstName','SexAtBirth','Count']
        
        # Add the current year column information
        current_file_contents['Year'] = current_file_year
        
        # Reorder the columns to match the output dataframe
        current_file_contents = current_file_contents[['Year','FirstName','SexAtBirth','Count']]
        
        # Append to the output dataframe
        output_df = pd.concat([output_df, current_file_contents], ignore_index = True)
        
#%% Basic Data Manipulation

# Format count to be an integer
output_df['Count'] = output_df['Count'].astype(int)

# Ensure firstname is all lowercase
output_df['FirstName'] = output_df['FirstName'].str.lower()

#%%

# Initialize list we will eventually append to
probability_fname_by_sexatbirth = []
# Create groupby Object
grouped_df = output_df.groupby('FirstName')

# Iterate through the groups.
#### index indicates iteration's firstname
#### frame indicates rest of iteration's data
for index, frame in grouped_df:
    
    # Get count of all people with that first name
    count_names = len(frame)
    
    # Get counts of male and female SexAtBirth peoples with that firstname over all years' files
    count_female = len(frame.query('SexAtBirth == "F"'))
    count_male = len(frame.query('SexAtBirth == "M"'))
    
    # Calculate percentages
    perc_female = count_female / count_names
    perc_male = count_male / count_names
    
    # Append to initialized empty list
    probability_fname_by_sexatbirth.append([index,perc_female,perc_male])
    
#%% Format probability dataframe

# Format the probability dataframe properly
probability_df = pd.DataFrame(np.array(probability_fname_by_sexatbirth))
probability_df.columns = ['FirstName','ProbabilityFemale','ProbabilityMale']

#%% Save ouputs

### Just in case people want to analyze it
output_df.to_csv('02_Wrangled/CountFnames_eachSexAtBirth_byYear.csv', index=False)

### For the naming process
probability_df.to_csv('02_Wrangled/ProbabilityFname_given_SexAtBirth.csv', index=False)

