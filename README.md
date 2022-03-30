# Aggregate static holdouts

## Goal 

Use the output mailfile from Mailplanner & run a python script that will map unique “misc” values to its respective holdout file. By mapping “misc” fields to a holdfile, we will output one holdout file that contains data for each market ID found in the mailfile. This aggregated holdout file will then be grouped by market ID to collect a quantity for each market ID, which can be used to copy/paste values into the matchback map file.

## Steps

Set your enviroment to the "data-team" directory
```
cd ~repo/matchbacks
```
Run the script "city_holdout_file.py" followed by two parameters:

```
Parameter 1: path to the mailfile that was downloaded from Mailplanner
Parameter 2: path to CLIENT_READY static holdouts directory
```
Full script ex:
```
python3 city_holdout_file.py {path to mailfile} {path to static holdouts}
```

## Outputs

Completion of the script will result in two files. File 1 will contain data for all holdout records. File 2 will summarize each holdout mailkey to be used in your map file. Example file output names:
```
SLMS105_2022-03-30_StaticHoldouts.csv
SLMS105_2022-03-30_StaticHoldouts_Summary.csv
```