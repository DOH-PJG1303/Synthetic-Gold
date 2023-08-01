# Misc Initialization README.md

This folder is designed to setup a couple state-specific aspects of the simulation before we can run it.

# START HERE - User Instructions

In order to complete these instructions, the following are all assumed:

* User has already cloned this repository and its scripts can be run locally.
* Python is installed and you can run Jupyter notebooks locally.
* You have access to Databricks. Compute gets rather large in file '05. Compile Addresses' and requires databricks/pyspark to run.
  * if you don't have databricks, you can still investigate methods, but will be unable to run the code in regular .py files without hitting memory errors.

Okay, now that you've read the assumptions, on to actually running scripts.

1. *Databricks* - Ensure that you have already adjusted the **Python Global Variables.py** script in the root directory to reflect the state you are interested in.  You'll want to adjust the following variables:
  * `state`
  *  `stateExtended`
  *  `stateFIPS`
  * `root_synthetic_gold`

2. *Databricks* - run **01. Compile Phone Numbers.py** script

3. *Databricks* - run **02. Proba Birth By Year By Age.py** script

4. *Local Python* - Navigate to **03. Wrangle FirstNames.ipynb**.  Adjust the following variables `state_abbreviation`, `state_name` `state_fips` to reflect your state.  Follow the formatting shown in terms of capitalization and data type.  Then run all cells in the notebook.

5. *Copying files* - Copy the following local files onto their respective cloud location (databricks accessible)
  * f'{root_synthetic_gold}/SupportingDocs/Names/03_Complete/firstname_probabilities.csv'

------------

# General Data Sources

* [Phone area codes](https://www.unitedstateszipcodes.org/zip-code-database/).  NOT FOR COMMERCIAL USE.
* Birth data a result of multiple combined datasets as described in the script documentation **{root}/Initialization/Births and Vital Stats** 
* [First name by race breakdown (download link)](https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/TYJKEZ)
* [First name by race breakdown (study/article)](https://www.nature.com/articles/sdata201825) for information on the article/study.
* Zip files for SSA First Name data
  * https://www.ssa.gov/oact/babynames/names.zip
  * https://www.ssa.gov/oact/babynames/state/namesbystate.zip
  * https://www.ssa.gov/oact/babynames/territory/namesbyterritory.zip
 
--------------

For a more detailed description of each file within this directory, I recommend examining the scripts yourself. 
Each has significant documentation that should be easy to follow.
If you have any questions, please reach out and email the contact given at the beginning of each script.

