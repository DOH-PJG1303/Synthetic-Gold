# Housing Initialization README.md

This folder is designed to setup the HousingLookup delta table used in the population simulation.
It requires manual intervention because each state will have a different looking HousingLookup table.

# START HERE - User Instructions

In order to complete these instructions, the following are all assumed:

* User has already cloned this repository and its scripts can be run locally.
* Python is installed and you can run Jupyter notebooks locally.
* You have access to Databricks. Compute gets rather large in file '05. Compile Addresses' and requires databricks/pyspark to run.
  * if you don't have databricks, you can still investigate methods, but will be unable to run the code in regular .py files without hitting memory errors.

Okay, now that you've read the assumptions, on to actually running scripts.

1. *Local Python* - Navigate to **00. Init Secret Census Key.ipynb**.  Follow the markdown information at the beginning of the notebook to get a census key and subsequently store your key in a secrets.pkl file.  Note that this file will be ignored by the repo's .gitignore file, but you should be careful who might have access to it normally.

2. *Local Python* - Navigate to **01. Housing TIGER scraping.ipynb**.  Adjust the `state` and `fips` variables according to your state. `state` should be the full state name with the first letter capitalized.  `fips` represents a 2 digit (0-pad if necessary) FIPS code for that state.  This can be done via google search if you don't already know your state's fips.  Run script and wait for completion.

3. *Local Python* - Navigate to **02. Pull Census Data - PUMS & Data Profiles.ipynb**.  Adjust the `stateFIPS` value in section 1.3 for your proper state.  Then run script and wait for completion.

4. *Local Python* - Navigate to **03. Wrangle Census Data.ipynb**.  Run script and wait for completion.

5. *Local Python* - Navigate to **04 Assign PUMS a ZIP via GeoCorr.ipynb**.  Meticulously follow markdown instructions at the top of the script. This will involve going to GeoCorr website, downloading data, and saving it with a very specific name.  Do not proceed until this is complete.  Then run script and wait for completion.

6. *Copying files* - Copy the following local files onto their respective cloud location (databricks accessible)
  * f'{root_synthetic_gold}/SupportingDocs/Housing/03_Complete/PUMS_with_zip.csv'
  * f'{root_synthetic_gold}/SupportingDocs/Housing/03_Complete/TIGER_state_streets.csv'
  * f'{root_synthetic_gold}/SupportingDocs/Housing/01_Raw/GeoCorr_mapping_hus.csv'

7. *Databricks* - Navigate to **05. Compile Addresses.py**.  Run script and wait for completion.

8. *Databricks* - Navigate to **06. Finalize Addresses.py**. Run script and wait for completion. Congratulations!!

------------

# General Data Sources

* [Census TIGER line files](https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html)
* [US Census Public Use Microdata Sample (PUMS)](https://www.census.gov/programs-surveys/acs/microdata.html)
* [American Community Survey (ACS) data profiles](https://www.census.gov/acs/www/data/data-tables-and-tools/data-profiles/)
* [Geocorr website](https://mcdc.missouri.edu/applications/geocorr2022.html)

------------

# File Overviews

### 01. Housing TIGER Scraping

The HousingLookup table uses state-specific, valid addresses.
We do this by using aggregating census TIGER data files for each county within the state and filtering to roads specific to "Local Neighborhood Road,
Rural Road, City Street" (see script 01.)
I should note that we use an API call to query these census TIGER data files.

<b>Data sources:</b> 
* [Census TIGER line files](https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html). Note we used 2019 data in the latest version of our simulation.  Subsequent years are expected to work, but there is a chance that they would require slight script-tweaking.

### 02. Pull Census Data - PUMS & Data Profiles

In this script we query PUMS data to get an idea for how many houses to exist in each PUMA area.
A PUMA is a geographic region who's borders are defined by the census.
By having this data, we can set up a HousingLookup table that generally reflects the distribution of houses seen in real life.

Note that we also query Census Data Profile data.
It can provide similar information to PUMS data, but when comparing the two, I found that PUMS is a better data source for this project.
The Data Profile data that we save is not used in the rest of this project, but is there for further research and investigation.

<b>Data sources:</b>
* [US Census Public Use Microdata Sample (PUMS)](https://www.census.gov/programs-surveys/acs/microdata.html). The Census PUMS datasets contain individual and housing unit records with anonymized data, making them incredibly useful for executing comprehensive statistical analyses on the US population.
* [American Community Survey (ACS) data profiles](https://www.census.gov/acs/www/data/data-tables-and-tools/data-profiles/). ACS data profiles complement this by providing summarized demographic, social, economic, and housing data.

### 03. Wrangle Census Data

This script serves to clean up the PUMS data pulled in the last script.
Field names and values are often coded, so here we uncode them and standardize field types.
The data also comes in a short-form that looks something like this:

| num_houses | PUMA |
| --- | --- |
| 3 | 00001 |

However, in this script we want each row to represent one house.
Not only do we want this, but we also want a surplus of houses in the instance of not running out of houses for someone to move into.
At one point in the script we define a variable called `multiplicative_surplus_value` within the `create_multiplicated_ids()` function.
This variable is self-defining.
When defined as `multiplicative_surplus_value = 2`, we'd see double the houses described in the PUMS dataset.
Considering our table example before with this multiplicative surplus value, we'd see the newly wrangled dataset:

| ID | num_houses | PUMA |
| --- | --- | --- |
| 1 | NULL | 00001 |
| 2 | NULL | 00001 |
| 3 | NULL | 00001 |
| 4 | NULL | 00001 |
| 5 | NULL | 00001 |
| 6 | NULL | 00001 |

Note that there are a whole lot more fields in this data than just those described in the example.
Here is a list of them: `['house_id', 'PUMA', 'BDSP', 'BLD', 'RMSP', 'TYPE', 'YBL', 'map_BLD', 'BLD_range_start', 'BLD_range_end', 'map_YBL', 'YBL_range_start', 'YBL_range_end']`

<b>Data sources:</b>
* Outputs from prior scripts.

### 04. Assign PUMS a ZIP via GeoCorr

This script serves to create probabilistically assign ZIP codes to houses within PUMA geographies.
Geocorr data provides how likely a house within a PUMA area falls within a given zip code.
We use these probabilities to assign houses their respective zip codes.


<b>Data sources:</b>
* [Geocorr website](https://mcdc.missouri.edu/applications/geocorr2022.html)
* Outputs from prior scripts

### 05. Compile Addresses

The primary function of this script is to combine our line-level house data with real address information.
We accomplish the following:
* Assign street addresses to line level data
* Probabilistically assign house year built YBL
* Probabilistically assign # of units per building given range

<b>Data sources:</b>
* Outputs from prior scripts.

### 06. Finalize Addresses

The purpose of this script is to take all of the preprocessed data and finalize it into the HousingLookup table.
Output columns include:

* house_id
* building_id
* road_section_id
* bld_type
* num_units
* num_beds
* num_rooms
* year_built
* address_number
* address_street
* unit_type
* unit_number
* zip
* city
* county_name
* county_fips
* puma
* area_code
* phone_landline
* state

