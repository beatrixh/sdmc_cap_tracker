# This repo contains code to pull metadata relating to CAPs

* `cap_sharepoint_links.txt` stores a list of relevant CAPs and links to their sharepoint folders
* `pull_from_sharepoint.py` pulls the header and filename of each CAP in the above list, from which it finds the current CAP version and last CAP revision date
* `pull_from_pdb.py` pulls CAP stage and "Open Date" from the PDB
* `main_pull_cap_metadata.py` calls the above scripts, formats the data and saves it to a staging file.

Smartsheet pulls from the staging file via data shuttle, updating the CAP at 5AM, 9AM, 1PM, and 5PM every day.

![CAP Updates Workflow](https://github.com/beatrixh/sdmc_cap_tracker/blob/master/cap_updates_diagram.png?raw=true)
