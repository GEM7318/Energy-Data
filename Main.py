
# Imports
import Crawler as cr
import FileHelper as fh

from selenium import webdriver
import os

# from importlib import reload
# reload(cr)
# reload(fh)

urls = fh.read_and_shuffle_hrefs()
# print(urls.keys())
# print(urls['WTI'])

browser = webdriver.Chrome(os.path.join(os.getcwd(), r'chromedriver.exe'))
browser.maximize_window()


dict_of_dfs = cr.get_dict_of_dfs(urls, browser)

df_total = fh.combine_scraped_dfs(dict_of_dfs)

fh.save_raw_file(df_total, 'Combined Output', 'outputs_csv')

browser.close()


import ETL

