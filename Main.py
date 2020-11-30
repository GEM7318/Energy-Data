# Imports
import Crawler as cr
import FileHelper as fh
import ETL as etl
import ETL_Combine_Processed as combine
import random
import time

import os
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager

cwd = os.path.join(os.getcwd().split("Energy-Scraping")[0], "Energy-Scraping")
os.chdir(cwd)

# from importlib import reload
# reload(cr)
# reload(fh)

# Random sleeping time so doesn't start at exact same time every night*****
intl_sleep = random.randint(0, 60 * 5)
print(
    f"<Sleeping for a randomly generated {intl_sleep} seconds before "
    f"beginning scraping>"
)
time.sleep(intl_sleep)

# Reading in URLS*****************
urls = fh.read_and_shuffle_hrefs()
# print(urls.keys())
# print(urls['WTI'])

# Scraping************************
# browser = webdriver.Chrome(os.path.join(os.getcwd(), r'chromedriver.exe'))
# browser = webdriver.Chrome(ChromeDriverManager().install())
try:
    browser = webdriver.Chrome(os.path.join(os.getcwd(), r"chromedriver.exe"))
except:
    browser = webdriver.Chrome(ChromeDriverManager().install())


browser.maximize_window()

dict_of_dfs = cr.get_dict_of_dfs(urls, browser)

# Combining daily results**********
df_total = fh.combine_scraped_dfs(dict_of_dfs)


fh.save_raw_file(df_total, "Combined Output", "outputs_csv")

# Getting most recently modified file*****
most_recently_modified_file = fh.get_path_to_most_recent_file()
# most_recently_modified_file


# Running through pipeline*********
etl.run_pipeline(most_recently_modified_file)

# Running the combining of all-processed files through initial ETL
project_path = os.path.join(os.getcwd(), "etl_outputs_xlsx")
user_path = r"D:\Dropbox\1 - CME Group Futures Files"
all_paths = [project_path, user_path]
# TODO: Change user_path such that it's based on a config file from Tom

base_file_nm = r"CME Group Futures Price - Prior Settle (COMBINED).xlsx"

combine.run_pipeline(all_paths, base_file_nm)


# ----------------------
# from pathlib import Path
#
# path = Path(r'C:\Users\GEM7318\Documents\Github\Energy-Scraping\outputs_csv')
# files = [file for file in path.iterdir()]
# files
# path2 = Path(r'D:\Dropbox\1 - CME Group Futures Files')
# files2 = [file for file in path2.iterdir()]
#
# to_etl = [file for file in files if file.name.split('.')[-1] == r'csv']
# # to_etl = [file for file in files if file.name.split('.')[-1] == r'.csv']
#
# dates = fh.get_distinct_dates_from_dir(str(path))
#
# files = [fh.get_latest_file_for_date(str(path), date) for date in dates]
#
# print(to_etl)
# for file in files:
#       print(file)
#
# for file in files:
#       etl.run_pipeline(file)
#       time.sleep(1)


# --------------------------------
# test = dict_of_dfs.get('Brent')
# dict_of_dfs.get('Brent').tail()

# col_df_dict = {}
# for k, v in dict_of_dfs.items():
#       cols = []
#       col_df_dict[k] = cols
#       for col in v.columns:
#             if isinstance(col, str):
#                   cols.append((col, col))
#                   # print(f"{k}: {col}")
#             else:
#                   cols.append(col)
#
# import pandas as pd
# for k, v in dict_of_dfs.items():
#       v.columns = pd.MultiIndex.from_tuples(col_df_dict[k])
#
# for k, v in dict_of_dfs.items():
#       for col in v.columns:
#             if isinstance(col, str):
#                   print(f"{k}: {col}")
#             else:
#                   pass
#
# for k, v in dict_of_dfs.items():
#       print(f"{k}:\n\t{v.columns}")
#
#
# col_df_dict.items()
#
# test = r'C:\Users\GEM7318\Documents\Github\Energy-Scraping\outputs_csv\2020-05-21 ~ Combined Output ~ v1.csv'
# test2 = r'C:\Users\GEM7318\Documents\Github\Energy-Scraping\outputs_csv\2020' \
#         r'-05-19 ~ Combined Output ~ v1.csv'
# etl.read_csv_from_path(test)
#
# df = pd.read_csv(test)
# df2 = pd.read_csv(test2)

# --------------------------------
