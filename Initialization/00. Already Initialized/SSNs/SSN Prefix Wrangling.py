# Import basic libs
import pandas as pd
import numpy as np

# Read in our data
df = pd.read_csv('SupportingDocs/SSN/02_Wrangled/ssnF3_crosswalk.csv', dtype=str)

# Prep output columns
xs = []
ys = []

# Loop through dataframe capturing our values along the way
for index in np.arange(0,len(df)):
    state = df.iloc[index,0]
    vals = df.iloc[index,1].replace('[','').replace(']','')
    
    # In the case of multiple ranges, loop through options
    for subindex in np.arange(0,len(vals.split(','))):
        curgroup = vals.split(',')[subindex-1]

        # Decipher the depicted range and append all potential options
        if '-' in curgroup:
            minNum = int(curgroup.split('-')[0])
            maxNum = int(curgroup.split('-')[1])
            allRange = np.arange(minNum,maxNum+1)
            for number in allRange:
                xs.append(state)
                ys.append(str(number).zfill(3))

        # If no range is provided, it is a single value, append to output
        else:
            xs.append(state)
            ys.append(curgroup.zfill(3))
                
# Format output
df_output = pd.DataFrame({'State':xs , 'Values':ys})

# Save output
df_output.to_csv('SupportingDocs/SSN/03_Complete/ssnF3_crosswalk_final.csv',index=False)