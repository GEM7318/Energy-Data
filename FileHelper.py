import os
from datetime import datetime
import random
import pandas as pd


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


