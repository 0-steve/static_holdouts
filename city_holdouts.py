import pandas as pd
import os
from datetime import date
import re

# parse misc column in mail file to collect unique market IDs
def parse_mailfile(mail_file_df):
    print("Mapping mailfile to holdouts...")
    print("")
    cols = list(mail_file_df.columns)
    lower_cols = [ name.lower().strip() for name in cols ]
    mail_file_df.columns = lower_cols
    uniq_misc = mail_file_df.misc.unique()
    misc_lst = [ misc.split("-")[1] for misc in uniq_misc ]

    return misc_lst

# map unique market IDs to a holdout ID
def get_holdout_id(mail_file_df, misc_df):
    miscs = parse_mailfile(mail_file_df)
    misc_df = misc_df[misc_df.marketID.isin(miscs)]

    return misc_df

# map holdout ID to holdout file name
def get_holdout_file(mail_file_df, misc_df, holdout_df):
    holdouts_to_use = get_holdout_id(mail_file_df, misc_df)
    holdout_df = holdout_df.merge(holdouts_to_use, left_on="Listcode", right_on="holdoutID", how="inner")
    holdout_df.drop("holdoutID", axis=1, inplace=True)

    return holdout_df

# use output from get_holdout_file and get unique holdout filenames
def get_unique_file_names(mail_file_df, misc_df, holdout_df):
    holdout_file_df = get_holdout_file(mail_file_df, misc_df, holdout_df)
    file_lst = list(holdout_file_df.originalFileName)

    return file_lst

# pass in file list and path to static holdout to convert files to dataframes
def file_to_df(path_to_dir, file_lst):
    main_path = os.getcwd()
    os.chdir(path_to_dir)
    dataframes = [ pd.read_csv(file, low_memory=False) for file in file_lst ]
    os.chdir(main_path)
    return dataframes, file_lst

# call file_to_df to update dataframes by creating file name column
def update_dataframes(path_to_dir, file_lst):
    dataframes = file_to_df(path_to_dir, file_lst)
    df_lst = dataframes[0]
    file_names = dataframes[1]

    for i, df in enumerate(df_lst):
        df['originalFileName'] = file_names[i]

    return df_lst

# call file_to_df to update dataframes & concat list of dataframes
def create_holdout_files(df_lst, file_lst):
    df_final = pd.concat(df_lst)
    df_final.Zip = df_final.Zip.apply(str)
    return df_final

def holdout_summary(df_lst, file_lst):
    df_counts = [ df.groupby(['Listcode', 'originalFileName']).size().reset_index(name='quantity') for df in df_lst ]
    concated_df = pd.concat(df_counts)
    concated_df = concated_df[["originalFileName", "Listcode", "quantity"]]

    return concated_df

def holdout_checks(final_df, holdout_df):
    holdout_counts = final_df.groupby(["Listcode", "originalFileName"]).size().reset_index(name="record_count")
    holdout_join = holdout_counts.merge(holdout_df, on=["Listcode", "originalFileName"], how="left")
    if holdout_join.quantity.equals(holdout_join.record_count):
        print("Holdout counts match")
        print("")
    else:
        print("Holdout counts are off!!")
        return holdout_join
        print("") 

def make_name(path):
    path = path.lower()
    path_components = path.split("/")
    ihd_lst = [ comp for comp in path_components if re.match(r'\d+-\d+-\d+', comp)]
    ihd = ihd_lst[0].replace(" ", "_") 
    client = path_components[-4].replace(" ", "_") 
    name = client + "_" + ihd
    return name

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('mail_file', type=str, help='the mailfile downloaded from mailplanner')
    parser.add_argument('path_to_dir', type=str, help='path to Static Holdouts')
    args = parser.parse_args()
    
    print("Loading files...")
    print("")
    clean_mail_file_df = args.mail_file.replace("\\","")
    mail_file_df = pd.read_csv(clean_mail_file_df, low_memory=False)
    misc_df = pd.read_pickle("citiesHoldout_marketID_2022-04-04.pkl")
    holdout_df = pd.read_pickle("citiesHoldout_summary_2022-04-04.pkl")

    holdout_files = get_unique_file_names(mail_file_df, misc_df, holdout_df)
    df_list = update_dataframes(args.path_to_dir, holdout_files)

    holdout_final_df = create_holdout_files(df_list, holdout_files)
    holdout_summmary_df = holdout_summary(df_list, holdout_files)

    current_date = date.today()
    name = make_name(args.mail_file)

    holdout_checks(holdout_final_df, holdout_df) # if fail then stop

    os.mkdir(f"/private/tmp/{name}") 
    os.chdir(f"/private/tmp/{name}")

    final_name = f"{name}_{current_date}_StaticHoldouts.csv"
    holdout_final_df.to_csv(final_name, index=False)
  
    summary_name = f"{name}_{current_date}_StaticHoldouts_Summary.csv"
    holdout_summmary_df.to_csv(summary_name, index=False)
    print(f"Files output in /private/tmp/{name}")
    print("")