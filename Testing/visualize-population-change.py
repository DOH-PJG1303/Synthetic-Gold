# -*- coding: utf-8 -*-
"""
Created on Wed Nov 23 10:35:19 2022

@author: PJ Gibson

@notes: Ensure that your working directory is {project_root}\Initialization\00. Already Initialized
"""
#%% Import libaries, fpaths

import numpy as np
import pandas as pd
import os
import matplotlib.pyplot as plt

fpath_birthRaw = '../../SupportingDocs/Births/01_Raw'

#%% 1. Load in data

### 1.1 Population Data
# Data on population by state by year (every 10 years), wrangle to integer population, and proper cols

df_populationCtRaw = pd.read_csv(f'{fpath_birthRaw}/apportionment.csv')
df_populationCtRaw['Population'] = df_populationCtRaw['Resident Population'].astype(str).replace('\,','', regex=True).astype(int)
df_populationCt = df_populationCtRaw.query('(`Geography Type` == "State")')\
                                    [['Name','Year','Population']]\
                                    .rename(columns={'Population':'PopulationUN',
                                                     'Name':'State'})

#%%

### 1.2 Deaths Data               
df_Death1 = pd.read_csv(f'{fpath_birthRaw}/Deaths by Year and State in 1968-1978.txt', delimiter='\t', skipfooter=10, engine='python')\
               .dropna(subset=['State'],how='any')
               
df_Death2 = pd.read_csv(f'{fpath_birthRaw}/Deaths-by-Year-and-State-1979-1998.txt', delimiter='\t', skipfooter=10, engine='python')\
               .dropna(subset=['State'],how='any')
               
df_Death3 = pd.read_csv(f'{fpath_birthRaw}/Deaths by Year and State in 1999-2020.txt', delimiter='\t', skipfooter=10, engine='python')\
               .dropna(subset=['State'],how='any')
               
df_Deaths = pd.concat([df_Death1,df_Death2,df_Death3])\
              [['Year','State','Deaths','Crude Rate','Population']]\
              .rename(columns={'Crude Rate':'MortalityRate'})

#%%

### 1.3 Births Data
df_Births1 = pd.read_csv(f'{fpath_birthRaw}/Births-by-Year-and-State-1995-2002.txt', delimiter='\t', skipfooter=10, engine='python')\
               .dropna(subset=['State'],how='any')

df_Births2 = pd.read_csv(f'{fpath_birthRaw}/Births-by-Year-and-State-2003-2006.txt', delimiter='\t', skipfooter=10, engine='python')\
               .dropna(subset=['State'],how='any')
               
df_Births3 = pd.read_csv(f'{fpath_birthRaw}/Births-by-Year-and-State-2007-2020.txt', delimiter='\t', skipfooter=10, engine='python')\
               .dropna(subset=['State'],how='any')            

df_Births = pd.concat([df_Births1,df_Births2,df_Births3])\
              [['Year','State','Births']]

#%% 2. Wrangle Data

df = df_Deaths.merge(df_Births, on=['State','Year'], how='left')\
              .merge(df_populationCt, on=['State','Year'], how='outer')

df['calculatedDeathRate'] = df.Deaths / (df.Population / 100_000)


#%%


df1 = df_Deaths.query('State == "Alaska"')
df2 = df_populationCt.query('State == "Alaska"')

plt.figure(figsize = (9,7))
plt.plot(df1.Year, df1.Population, color='b')
plt.plot(df2.Year, df2.PopulationUN, color='r')
plt.scatter(df2.Year, df2.PopulationUN, color='darkred',s=5)


plt.show()