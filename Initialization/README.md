# Initialization

If you've already set up the user instructions as described in the root directory's README.md file, you're in the right place!
Otherwise, **please return to the root directory** and follow instructions.
The simulation will not work as expected if you navigate directions out of order.

## 1. Folder Setup

Here is the information copied from the root's README.md file that describes each folder and a general depiction of its contents.

**Numbering**: Folders and files within folders have a numbering file/folder name format.
This indicates the order in which they were ran or how they should be ran.

* `00. Already Initialized` - This folder and sub-folders contain local jupyter notebooks and databricks python notebooks that have already been ran and produced output files. Users may be interested in the methodology of this section as most of the preprocessing is done in this folder.

* `01. Misc` - This folder contains miscellaneous files that the user will need to tweak (state-specific) and then run. The scripts are not necissarily specific to one process, hence the folder name.

* `02. Housing` - This folder contains several scripts that end up creating the *HousingLookup/Delta* table used in the simulation.  Early scripts need to be tweaked to provide the right addresses for your specific simulated state.

* `03. Table Setup` - This folder contains scripts that initialize the tables that interact with one another during the simulation.  

* *04. Run Simulation.py* - This script triggers the simuluation run.  Note that with the given cluster size, this process took a little under 8 hours to run to completion.

* *05. Create LongSyntheticGold.py* - This script takes the *Population/Delta* table's various delta-versions and creates one "long" dataset that contains each year's snapshot population between 1921-2021 (inclusive). 

* *06. Create DB and tables.py* - This script allows a user to make a database and tables in their databricks Hive MetaStore.  

# 2. User Instructions

You do not need to run any notebooks in the folder `00. Already Initialized/`.  

1. Navigate to the `01. Misc/` directory and follow instructions listed in that folder's README.md file. Do not move on until finished.
2. Navigate to the `02. Housing/` directory and follow instructions listed in that folder's README.md file. Do not move on until finished.
3. Navigate to the `03. Table Setup/` directory and follow instructions listed in that folder's README.md file. Do not move on until finished.
4. Run the *04. Run Simulation.py* file in databricks.  Do not run the next scripts until this has completed successfully. Note: For Washington state, it took around 7.5 hours given the configured cluster described in the root README.md file.  For larger states it may take more time, for smaller states it may take less time.  Please contact PJ if you perform this simulation for a non-Washington state with this notebook's runtime for shared knowledge and understanding.
5. Run the *05. Create LongSyntheticGold.py* file in databricks.  Do not run the next scripts until this has completed successfully.
6. Run the *06. Create DB and tables.py* file in databricks.  Do not run the next scripts until this has completed successfully.

Congratulations, at this point you should have a synthetic population!!!!
Explore your cloud data location `{root_directory}/Data` and begin playing with the data :)

[Congrats GIF](../SupportingDocs/Images/leo_congrats.gif)