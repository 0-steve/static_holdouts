import pandas as pd
import os

# parse misc column in mail file to collect unique market IDs
def parse_mailfile(mail_file):
    mail_file_df = pd.read_csv(mail_file, low_memory=False)
    cols = list(mail_file_df.columns)
    lower_cols = [ name.lower() for name in cols ]
    mail_file_df.columns = lower_cols
    uniq_misc = mail_file_df.misc.unique()
    misc_lst = [ misc.split("-")[1] for misc in uniq_misc ]

    return misc_lst

# map unique market IDs to a holdout ID
def get_holdout_id(mail_file):
    misc_df = pd.read_csv("city_hold_market_id_2022-03-17.csv", low_memory=False)
    miscs = parse_mailfile(mail_file)
    misc_df = misc_df[misc_df.marketID.isin(miscs)]

    return misc_df

# map holdout ID to holdout file name
def get_holdout_file(mail_file):
    holdout_df = pd.read_csv("citiesHoldout_counts_2022-03-08.csv", low_memory=False)
    holdouts_to_use = get_holdout_id(mail_file)
    holdout_df = holdout_df.merge(holdouts_to_use, left_on="Listcode", right_on="holdoutID", how="inner")
    holdout_df.drop("holdoutID", axis=1, inplace=True)

    return holdout_df

# use output from get_holdout_file and get unique holdout filenames
def get_unique_file_names(mail_file):
    holdout_file_df = get_holdout_file(mail_file)
    file_lst = list(holdout_file_df.originalFileName)

    return file_lst

# pass in file list and path to static holdout to convert files to dataframes
def file_to_df(path_to_dir, mail_file):    
    file_lst = get_unique_file_names(mail_file)
    os.chdir(path_to_dir)
    dataframes = [ pd.read_csv(file, low_memory=False) for file in file_lst ]
    return dataframes, file_lst

# call file_to_df to update dataframes by creating file name column
def update_dataframes(path_to_dir, mail_file):
    dataframes = file_to_df(path_to_dir, mail_file)
    df_lst = dataframes[0]
    file_names = dataframes[1]

    for i, df in enumerate(df_lst):
        df['originalFileName'] = file_names[i]

    return df_lst

# call file_to_df to update dataframes & concat list of dataframes
def create_holdout_files(path_to_dir, mail_file):
    df_lst = update_dataframes(path_to_dir, mail_file)
    df_final = pd.concat(df_lst)
    return df_final

def final_touch(path_to_dir, mail_file):
    dataframes = update_dataframes(path_to_dir, mail_file)
    df_counts = [ df.groupby(['Listcode', 'originalFileName']).size().reset_index(name='quantity') for df in dataframes ]
    concated_df = pd.concat(df_counts)
    concated_df = concated_df[["originalFileName", "Listcode", "quantity"]]

    return concated_df


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('mail_file', type=str, help='the mailfile downloaded from mailplanner')
    parser.add_argument('path_to_dir', help='path to Static Holdouts')
    args = parser.parse_args()

    holdout_final_df = create_holdout_files(args.path_to_dir, args.mail_file)
    holdout_summmary_df = final_touch(args.path_to_dir, args.mail_file)

    holdout_final_df.to_csv("something.csv", index=False)
    holdout_summmary_df.to_csv("something_else.csv", index=False)