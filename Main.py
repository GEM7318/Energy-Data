
# Imports
import Crawler as cr
import FileHelper as fh
import ETL as etl

from selenium import webdriver
import os

# from importlib import reload
# reload(cr)
# reload(fh)

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

# Running through pipeline*********
most_recently_modified_file = fh.get_path_to_most_recent_file()

etl.run_pipeline(most_recently_modified_file)


