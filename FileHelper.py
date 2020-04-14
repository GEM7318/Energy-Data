import os
from datetime import datetime
import random
import pandas as pd
import re


def read_and_shuffle_hrefs(file_nm=r'urls.csv'):
    """
    Reads in csv of names and hrefs and returns shuffled dictionary to
    traverse.
    """
    path_to_read = os.path.join(os.getcwd(), file_nm)
    urls_df = pd.read_csv(path_to_read)

    urls = urls_df.set_index('Name')['Href'].to_dict()

    names = [val for val in urls.keys()]
    random.shuffle(names)
    shuffled_urls = {k: urls[k] for k in names}

    return shuffled_urls


# urls = read_and_shuffle_hrefs()
# print(urls.keys())


def combine_scraped_dfs(dict_of_dfs: dict) -> pd.DataFrame:
    """
    Receives dictionary of DataFrames and combines into a single DataFrame.
    Strips out the last record of every df that was observed to be junk/a
    descriptor appended to all of them
    :param dict_of_dfs: Dictionary of metric name to DataFrame containing
    the futures by month for that metric
    :return: A single DataFrame containing futures by month for all metrics
    """
    df_total = pd.DataFrame()

    for _, v in dict_of_dfs.items():
        v.drop(v.tail(1).index, inplace=True)
        df_total = df_total.append(v)

    return df_total


def get_file_name(folder_ext: str, file_name: str, is_etl=False) -> str:
    """
    Creates a file name with an index number based on folder extension and
    file name inputs.
    :param folder_ext: Underscore-delimited name of folder with the last
    argument being the file type
    :param file_name: Base name of file
    :return: File name including base file name, date, and file index number
    within a given day
    """
    base_path = os.path.join(os.getcwd(), folder_ext)
    current_date = str(datetime.today()).split(' ')[0]

    files = os.listdir(base_path)
    pre_existing_files = [file for file in files if
                          file_name in file and current_date in file]
    index_num = len(pre_existing_files) + 1

    file_ext = folder_ext.split('_')[-1]

    if not is_etl:
        file_name = f"{current_date} ~ {file_name} ~ v{index_num}.{file_ext}"
    else:
        file_name = f"{file_name} {current_date}.{file_ext}"

    return file_name


# get_file_name('outputs_csv', 'Daily Total')
# get_file_name('_txt', 'Brent')


def save_raw_file(data, file_name, folder_ext):
    """
    Accepts data (string of HTML or DataFrame) and writes out to local text
    file in appropriate place.
    :param data: HTML or DataFrame
    :param folder_ext: Base folder name to save raw html in
    :param file_name: Name of Href or File to save
    :return: None
    """
    file_name = get_file_name(folder_ext, file_name)
    path_to_write = os.path.join(os.getcwd(), folder_ext, file_name)

    file_ext = os.path.splitext(file_name)[-1]

    if file_ext == r'.txt':

        with open(path_to_write, 'w', encoding='utf-8') as f:
            f.write(data)

    elif file_ext == r'.csv':

        data.to_csv(path_to_write, index=False)

    print(f"\t<local file saved to {path_to_write}>")

    return None


# df = pd.DataFrame()
# save_raw_file(df, 'Combined Total', 'outputs_csv')

# sample_str = 'Sample string'
# save_raw_file(sample_str, 'Test', '_txt')


def get_path_to_most_recent_file(folder_ext=r'outputs_csv'):
    """
    Imports most recently modified raw output csv as a DataFrame
    :return: DataFrame
    """
    base_path = os.path.join(os.getcwd(), folder_ext)
    full_paths = [os.path.join(base_path, val) for val in
                  os.listdir(base_path)]
    file_paths = [val for val in full_paths if os.path.isfile(val)]

    mod_file_dict = {os.path.getmtime(path): path for path in file_paths}
    most_recent_mod = \
        mod_file_dict[sorted(mod_file_dict.keys(), reverse=True)[0]]

    return most_recent_mod


def get_latest_output_for_date(date_str):
    """
    Imports most recently modified raw output csv for a given day.
    """
    base_path = os.path.join(os.getcwd(), r'outputs_csv')
    full_paths = [os.path.join(base_path, val) for val in
                  os.listdir(base_path)]
    file_paths = [val for val in full_paths if os.path.isfile(val)]

    mod_file_dict = {os.path.getmtime(path): path for path in file_paths}
    most_recent_mod = \
        mod_file_dict[sorted(mod_file_dict.keys(), reverse=True)[0]]

    df = pd.read_csv(most_recent_mod)
    df.drop(df.head(1).index, inplace=True)
    print(f"Imported:\n\t\t{most_recent_mod}")

    return df

# date_in = '2020-04-06'
# test_str = '2020-04-06 ~ Combined Output ~ v3.csv'
# date_str, _, version = test_str.split(' ~ ')
# version_num = int(version[::-len(version)])
# files = os.listdir(r'C:\Users\GEM7318\Documents\Github\Energy-Scraping'
#                    r'\outputs_csv')
# days_files = [val for val in files if date_in in val]


def get_latest_file_for_date(dir_str: str, date_str: str) -> str:
    """
    Gets full path to the 'latest' file name by version number for all files
    within a given directory on a given date based on the version number.
    :param dir_str: Directory to traverse
    :param date_str: Date to partition by
    :return: Path to file that has the highest version number on the given
    date
    """
    files = [file for file in os.listdir(dir_str) if date_str in file]

    file_version_dict = {}
    for file in files:
        date_str, _, version = os.path.splitext(file)[0].split(' ~ ')
        version_num = int(version[::-len(version)])
        file_version_dict[version_num] = file

    latest_file = file_version_dict[list(file_version_dict.keys())[::-1][0]]
    latest_path = os.path.join(dir_str, latest_file)

    return latest_path
# outputs_dir = os.path.join(os.getcwd(), 'outputs_csv')
# get_latest_file_for_date(outputs_dir, '2020-04-10')
# get_latest_file_for_date(outputs_dir, '2020-04-06')


def get_distinct_dates_from_dir(dir_str: str) -> list:
    """
    Traverses a directory following tilda-delimited naming convention with date
    as first argument and returns distinct dates within directory
    :param dir_str: Directory to traverse
    :return: Sorted list of distinct dates within the directory
    """
    files = [file for file in os.listdir(dir_str) if re.findall('~', file)]
    dates = {file.split('~')[0] for file in files}
    dates = sorted(list(dates))

    return dates
# get_distinct_dates_from_dir(outputs_dir)


def file_checker(path_to_file: str) -> bool:
    """
    Simple function to test whether or not file is open.
    :param path_to_file: Path to file
    :return: Boolean indicator of file being currently closed/writable or not
    """
    is_okay = True
    try:
        tester = open(path_to_file, "w")
        tester.close()
    except IOError:
        is_okay = False

    return is_okay
