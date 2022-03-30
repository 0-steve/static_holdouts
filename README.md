# Aggregate static holdouts

## Goal 

Use the output mailfile from Mailplanner & run a python script that will map unique “misc” values to its respective holdout file. By mapping “misc” fields to a holdfile, we will output one holdout file that contains data for each market ID found in the mailfile. This aggregated holdout file will then be grouped by market ID to collect a quantity for each market ID, which can be used to copy/paste values into the matchback map file.
