
# Imports
import Crawler as cr
import FileHelper as fh
import ETL as etl
import ETL_Combine_Processed as combine
import random
import time

from selenium import webdriver
import os

cwd = os.path.join(os.getcwd().split('Energy-Scraping')[0], 'Energy-Scraping')
os.chdir(cwd)

# from importlib import reload
# reload(cr)
# reload(fh)

# Random sleeping time so doesn't start at exact same time every night*****
intl_sleep = random.randint(0, 60 * 5)
print(f"<Sleeping for a randomly generated {intl_sleep} seconds>")
time.sleep(intl_sleep)

# Reading in URLS*****************
urls = fh.read_and_shuffle_hrefs()
# print(urls.keys())
# print(urls['WTI'])

# Scraping************************
browser = webdriver.Chrome(os.path.join(os.getcwd(), r'chromedriver.exe'))

browser.maximize_window()

dict_of_dfs = cr.get_dict_of_dfs(urls, browser)

# Combining daily results**********
df_total = fh.combine_scraped_dfs(dict_of_dfs)

fh.save_raw_file(df_total, 'Combined Output', 'outputs_csv')

# Getting most recently modified file*****
most_recently_modified_file = fh.get_path_to_most_recent_file()

# Running through pipeline*********
etl.run_pipeline(most_recently_modified_file)

# Running the combining of all-processed files through initial ETL

project_path = os.path.join(os.getcwd(), 'etl_outputs_xlsx')
user_path = r'C:\Users\GEM7318\Dropbox\1 - CME Group Futures Files'
# TODO: Change user_path such that it's based on a config file from Tom
all_paths = [project_path, user_path]

combine.run_pipeline(all_paths, r'CME Group Futures Price - Prior Settle ('
                             r'COMBINED).xlsx')