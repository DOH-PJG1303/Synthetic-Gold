# Functions

This folder contains only 3 files, all Databricks python files that either guide the process or can be applied to the process' output.
See descriptions below (same as root README descriptions)

* *New Process Functions.py* - These functions serve as the heart of the simulation.  It dictates which tables interact with one another and which processes run before others.  It's very dense, but well-documented.  For visual learners, please see the document *{root}/SupportingDocs/process_visualizations.vsdx* for a table-by-table breakdown of what all is impacted during the various sub-processes of each year's simulation.  It is a visio document.

* *State Management Functions.py* - These functions help keep track of our delta table versions as the process runs.  Occasionally you may run out of memory or hit some roadblock during the run of the simulation.  In that case, you can always rerun the simulation from the last successful year by using the `reset_state(year)` function which reverts all delta tables to their version at the end of that year.  It serves so that a user doesn't need to restart the entire process if it fails halfway through the year 2000.

* *Dirty Functions.py* - These functions allow a user to apply real-world type messiness (or dirty-ness) to data. All a user needs to do is specify a python dictionary of how they want to messy up their data.