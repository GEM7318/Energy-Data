import pandas as pd
import os
import hashlib
import math
import ETL as etl
import FileHelper as fh

import logging

logging.basicConfig(filename='test.log', level=logging.DEBUG)


# Steps to build for initial concatenation-------------------------------------
# 1: Read in all dataframes as dictionary values
# 2: Create another dictionary of like-keys with hashed values for the columns
# of the corresponding dataframes
# 3: Create list of hashed values turner Counter dictionary and store the most
# used hash value
# 4: Iterate through all, checking that a given DataFrame's hashed columns
# matches the most common hash before inserting and raise passable


# Steps to build for daily update
# 1: Identify most recently uploaded file
# 2: Read in both most recent DF and master combined DF
# 3: Generate hash of both DataFrame's columns
# 4: Verify that the new file's columns match the main file's columns,
# append one on top of the other and write back out to Excel

# def quick_hash(unhashed):
#     """Quick hashing function to truncate the length playlist_id:
#     snapshot_id key.
#     """
#     hashed_base = hashlib.md5(unhashed.encode('utf-8'))
#     hashed_str = hashed_base.hexdigest()
#     return hashed_str


def hash_from_cols(df):
    """Quick hashing function to return hash value from a DataFrame's columns.
    """
    to_hash = '~'.join(df.columns.to_list())

    hash_base = hashlib.md5(to_hash.encode('utf-8'))
    hashed = hash_base.hexdigest()

    return hashed

# template_df = pd.read_excel()
# valid_col_hash = hash_from_cols(template_df)
# TODO: Change to get column_template_hash()

def get_valid_hash(path_to_template=None):

    if not path_to_template:
        path_to_template = \
            os.path.join(os.getcwd().split('Energy-Scraping')[0],
                         'Energy-Scraping', 'ETL_Output_Template.xlsx')
    else:
        pass

    df = pd.read_excel(path_to_template)
    template_hash = hash_from_cols(df)

    return template_hash
# get_valid_hash()


def get_paths_to_base_etl_outputs(path_to_read=None):

    if not path_to_read:
        path_to_read = os.path.join(os.getcwd(), 'etl_outputs_xlsx')

    else:
        pass

    paths = [os.path.join(path_to_read, file)
             for file in os.listdir(path_to_read)
             if r'.xlsx' in file and 'combined' not in file.lower()]

    return paths


def get_dict_of_dfs(list_of_paths: list) -> dict:
    """
    Returns a dictionary of DataFrames from a list of paths
    :param list_of_paths: List of full directory paths to .xlsx files
    :return: Dictionary of Df_Name: pd.DataFrame based
    """
    df_dict = {}
    for path in list_of_paths:
        print(path)
        df_dict[os.path.split(path)[-1].split('.')[0]] = pd.read_excel(path)

    return df_dict


def get_col_check_dtl(dict_of_dfs):
    """
    Returns a dictionary of df_name: hash value for columns.
    :param dict_of_dfs:
    :return:
    """
    template_hash = get_valid_hash()

    hash_dict = {}
    for k, v in dict_of_dfs.items():
        hash_dict[k] = hash_from_cols(v) == template_hash

    return hash_dict


def get_list_of_indicators(df,
                           cols_to_check=['WTI', 'USGC-ULSD', 'USGC-HSFO']):
    """
    Gets a list of 1s and 0s to indicate invalid/valid records of a DataFrame
    based on numeric values of specified columns
    :param df:
    :param cols_to_check:
    :return:
    """
    unusable_list = []
    for vals in df[cols_to_check].iterrows():
        valid = [v for v in vals[1] if not math.isnan(v)]

        if len(valid) != len(vals[1]):
            unusable_list.append(1)

        else:
            unusable_list.append(0)

    return unusable_list


def truncate_df(df):
    """
    Truncates a DataFrame by cutting chopping at the index position where all
    values within a list are 1s from that point forward
    :param df:
    :param list_of_indicators:
    :return:
    """
    list_of_indicators = get_list_of_indicators(df)

    for i, val in enumerate(list_of_indicators):

        total_less_current = len(list_of_indicators) - i
        remaining_unusable = sum(list_of_indicators[i:])

        if total_less_current == remaining_unusable:
            last_valid_index = i
            df2 = df.iloc[0:(last_valid_index - 1)]
            break

        else:
            df2 = df

    return df2


def combine_valid_dfs(dict_of_dfs, hash_dict):
    """
    Combines multiple DataFrames into a single DataFrame and valids the
    columns of each against a hash_dict parameter
    :param dict_of_dfs:
    :param hash_dict:
    :return:
    """
    dict_of_trunc_dfs = {k: truncate_df(v) for k, v in dict_of_dfs.items()}
    combined_df = pd.concat([dict_of_trunc_dfs[k]
                             for k, v in hash_dict.items() if v])

    invalid_dfs = [k for k, v in hash_dict.items() if not v]
    try:
        invalid_dfs == []
    except Exception as e:
        logging.exception(f"Could not load invalid DataFrames: {invalid_dfs}",
                          e)

    combined_df.reset_index(drop=True, inplace=True)

    return combined_df


# TODO: Add logic to log dataframes that can't be loaded due to column mismatch


def get_context_for_combined(df_data: pd.DataFrame) -> pd.DataFrame:
    """
    Generates the DataFrame for 'Context' tab of ETL output as well as
    extracts and re-formats the 'collected date'.
    :param df_data: Pivoted DataFrame for primary output tab
    :return: DataFrame to populate 'Context' tab and the formatted date for
    file name
    """
    path_to_urls = os.path.join(os.getcwd(), r'urls.csv')
    context_df = pd.read_csv(path_to_urls)

    list_of_dates = sorted(list(set(df_data['Collected Date'])))
    first_date, last_date = list_of_dates[0], list_of_dates[-1]

    source_date_df = \
        pd.DataFrame(data=['CME Group (Prior Settle)', first_date, last_date],
                     index=['Source', 'First Date in File',
                            'Last Date in File'], columns=['Href'])
    source_date_df = source_date_df.reset_index()
    source_date_df.rename(columns={'index': 'Name'}, inplace=True)

    context_df = pd.concat([source_date_df, context_df])

    return context_df


def write_combined_dict(dfs,
                        base_file_name=r"CME Group Futures Price - Prior "
                                       r"Settle (COMBINED).xlsx",
                        base_path=os.path.join(os.getcwd(),
                                               'etl_outputs_xlsx')):
    base_file_name, folder_ext = base_file_name.split('.')
    initial_file_name = f"{base_file_name}.xlsx"
    initial_write_path = os.path.join(base_path, initial_file_name)
    print(initial_write_path)

    if fh.file_checker(initial_write_path):
        etl.fancy_excel_writer(initial_write_path, dfs)

    else:
        try:
            except_file_nm = fh.get_file_name('etl_outputs_xlsx',
                                              base_file_name)
            except_write_path = os.path.join(base_path, except_file_nm)
            etl.fancy_excel_writer(except_write_path, dfs)

        except Exception as e:
            logging.exception('Exception Occurred: Could not write to main '
                              'path', e)

    return None
# TODO: Change fh.get_file_name() above such that it will continue to
#  function if using a different directory structured


def run_pipeline(paths_to_write_to: list,
                 base_file_name: str =
                 r'CME Group Futures Price - Prior Settle (COMBINED).xlsx'):

    paths = get_paths_to_base_etl_outputs()

    df_dict = get_dict_of_dfs(paths)

    hash_dict = get_col_check_dtl(df_dict)

    df_total = combine_valid_dfs(df_dict, hash_dict)

    dfs = {'Combined_Vertical': df_total,
           'Context': get_context_for_combined(df_total)}

    for path in paths_to_write_to:
        write_combined_dict(dfs, base_file_name, path)

    print(f"<Combine-All Pipeline Completed>")


project_path = os.path.join(os.getcwd(), 'etl_outputs_xlsx')
base_file_nm = r'CME Group Futures Price - Prior Settle (COMBINED).xlsx'
# user_path = r'C:\Users\GEM7318\Dropbox\1 - CME Group Futures Files'
# all_paths = [project_path, user_path]

# run_pipeline([project_path], base_file_nm)

# run_pipeline(all_paths, r'CME Group Futures Price - Prior Settle ('
#                              r'COMBINED).xlsx')
