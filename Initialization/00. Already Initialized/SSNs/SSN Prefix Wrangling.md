# SSN Prefix Wrangling

The purpose of this document is to walk through the steps that I took to get a list of available first 3 SSN digits (we will refer to as SSN F3) by state.

----------------------

<p>Author: PJ Gibson</p>
<p>Date: 2022-09-14</p>
<p>Contact: peter.gibson@doh.wa.gov</p>
<p>Other Contact: pjgibson25@gmail.com</p>

----------------------

## 1. Finding the source

There are several sources out there that contain at least part of the information that I desired.
As more and more people are born, some larger states such as California need to adopt new SSN values before they run out.
In the example of California:

> California originally had SSN F3 values of 545-573
>
> California later accepted SSN F3 values of 602-626 considering population growth


Unfortunately the SSN.org website depicting acceptable SSN F3 values, linked [here](https://www.ssa.gov/employer/stateweb.htm), only shows California as having values of 545-573.
This doesn't depict the full picture.

Additionally, other websites such as a SSN verfify website, linked [here](https://www.ssn-verify.com/lookup/california), depict California having values of 545-626.
This also doesn't depict the full picture because the range should exclude some values, specifically values of 574-601 inclusive.

I did find a resource that contains all of the information that we desire.
This is a [link to the accepted source](https://www.uclaisap.org/trackingmanual/manual/appendix-G.html) of our SSN F3 information by state.
This source has information on groups of acceptable ranges for states, giving us the full picture of acceptable values.
This information was obtained by merging information from the SSN.gov website with a FOIA (Freedom of Information Act) request currently unavailable to the public.
The group that seemingly performed to the FOIA request was a UCLA group working on a project labeled "UCLA Integrated Substance Abuse Programs".

## 2. Extracting data from the source

The html source that was chosen does not allow for python API pulls, meaning that we can't extract the data directly using the python requests package.
We could have used Python's [Selenium](https://selenium-python.readthedocs.io/) package to extract the information, but I decided against it.
Databricks and Selenium aren't necissarily meant to be used together and Selenium requires some user configuration (like installing a chromedriver) that I don't want other people to have to navigate through.

Instead, I opted for a copy/paste into Visual Studio Code where I could perform some regular expressions transormations to create a .csv file manually.

### 2.1 Copy and Paste

Below is the data that I copy and pasted, [full credit to the UCLA Integrated Substance Abuse Programs](https://www.uclaisap.org/trackingmanual/index.html).

------------------

1 st 3 Digits

State

001-003

New Hampshire

004-007

Maine

008-009

Vermont

010-034

Massachusetts

035-039

Rhode Island

040-049

Connecticut

050-134

New York

135-158

New Jersey

159-211

Pennsylvania

212-220

Maryland

221-222

Delaware

223-231

Virginia

691-699*

232-236

West Virginia

232

North Carolina

237-246

681-690

247-251

South Carolina

654-658

252-260

Georgia

667-675

261-267

Florida

589-595

766-772

268-302

Ohio

1 st 3 Digits

State

303-317

Indiana

318-361

Illinois

362-386

Michigan

387-399

Wisconsin

400-407

Kentucky

408-415

Tennessee

756-763*

416-424

Alabama

425-428

Mississippi

587-588

752-755*

429-432

Arkansas

676-679

433-439

Louisiana

659-665

440-448

Oklahoma

449-467

Texas

627-645

468-477

Minnesota

478-485

Iowa

486-500

Missouri

501-502

North Dakota

503-504

South Dakota

505-508

Nebraska

509-515

Kansas

516-517

Montana

518-519

Idaho

520

Wyoming

521-524

Colorado

650-653

1 st 3 Digits

State

525,585

New Mexico

648-649

526-527

Arizona

600-601

764-765

528-529

Utah

646-647

530

Nevada

680

531-539

Washington

540-544

Oregon

545-573

California

602-626

574

Alaska

575-576

Hawaii

750-751*

577-579

District of Columbia

580

Virgin Islands

580-584

Puerto Rico

596-599

586

Guam

586

American Samoa

586

Philippine Islands

700-728

Railroad Board**

729-733

Enumeration at Entry

-----------------------

### 2.2 Wrangle with Regular Expressions

Feel free to copy these steps in your own Visual Studio Code document to see how the transformations are done.
You'd need to copy and paste the copy/pasted data into an empty document in visual studio code.

1. Press `ctrl + f` to get into find mode.
   - Click on the down arrow to operate find & replace mode
   - Ensure that the "Use Regular Expressions" box is enabled.  It is represented by a  lower left box and a upper right astrix within the find and replace box.
2. Replace `(1 st 3 Digits)|(^\n)|(^state)` with nothing. Do it again
3. Replace `\*` with nothing
4. Replace `(^\d.*)\n(.*)` with `$2,$1`
5. Replace `\n(([\d\-]+,){1,2})([\d\-]+$)\n(.*)` with `,$3\n$4,$1`
6. Replace `((?<=,),)|(,$)` with nothing
7. Replace `([^,]+),(.*)` with `$1,"[$2]"`
8. Add the line `State,Range` as the first line of the file
8. Save output as `{root}/SupportingDocs/SSN/02_Wrangled/ssnF3_crosswalk.csv`

### 2.3 Wrangle with Python

Below is the copy and pasted file that I used to format the .csv file further.

```python
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
```

Now the output is normalized and clean, with an appearance like:

| State | Values |
| ---- | ---- |
| New Hampshire | 001 |
| New Hampshire | 002 | 
| New Hampshire | 003 |
| Maine | 004 |
| ... | ... |

and we can query the data with an operation as simple as:
```python
acceptable_vals = df.query(f'State == "{state}"')['Values]
```