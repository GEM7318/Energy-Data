# Imports
from selenium import webdriver
import random
from bs4 import BeautifulSoup
import lxml
import pandas as pd
import os
import time
import random
from datetime import datetime

# Importing urls as dataframe
urls_df = pd.read_csv('urls.csv')

# Converting to dictionary
urls = urls_df.set_index('Name')['Href'].to_dict()
print(urls)

# Instantiating browser instance
browser = webdriver.Chrome(os.path.join(os.getcwd(), r'chromedriver.exe'))


def html_from_javascript(href: str, lower: int=20, upper: int=30):
    """
    Retrieves html from web page given an href, delaying for javascript to
    fire and load data.
    :param lower: Lower bound of seconds to sleep for
    :param upper: Upper bound of seconds to sleep for
    :param href: href of web page
    :return: string, string
    """
    browser.get(href)
    time_to_sleep = random.randint(lower, upper)
    print(f"\t<page opened - started sleeping for {time_to_sleep} before "
          f"downloading page data>")
    time.sleep(time_to_sleep)
    print('\t<downloaded of page data completed>')
    html = browser.page_source
    current_tmstmp = str(datetime.today())
    return html, current_tmstmp


def df_from_html(html: str, href_name: str, current_tmstmp: str):
    """
    Parses the first table out of a BeautifulSoup object set of HTML and loads
    into DataFrame - returns df and prettified soup string.
    :param html: Soup object
    :param href_name: Name of href for df column
    :param current_tmstmp: String of current UTC timestamp
    :return: DataFrame, string
    """
    soup = BeautifulSoup(html, 'html.parser')
    soup_prettified = str(soup.prettify())
    html_tables = soup.find_all('table')
    df = pd.read_html(str(html_tables))[0]
    df.insert(0, 'metric_id', href_name)
    df['collected_tmstmp'] = current_tmstmp
    print("\t<parsed HTML into dataframe>")
    return df, soup_prettified


def save_raw_html(prettified_html, href_name):
    """
    Accepts prettified string of raw HTML and writes out to local text file.
    :param prettified_html: String of HTML
    :param href_name: Name of href from which the HTML was pulled (used in
    file name)
    :return: None
    """
    current_date = str(datetime.today()).split(' ')[0]
    path_to_write = os.path.join(os.getcwd(), '_txt',
                                 f'{current_date} - {href_name}.txt')

    with open(path_to_write, 'w', encoding='utf-8') as f:
        f.write(prettified_html)
    print("\t<raw html written to local text file>")

    return None


def get_dict_of_dfs(dict_of_hrefs):

    dict_of_dfs = {}
    for href_name, href in dict_of_hrefs.items():
        print(f"Scraping started for: {href_name}")
        raw_html, current_tmstmp = \
            html_from_javascript(href)

        df, prettified_soup = df_from_html(raw_html, href_name, current_tmstmp)

        save_raw_html(prettified_soup, href_name)
        dict_of_dfs[href_name] = df
        time_to_sleep = random.randint(5,15)
        time.sleep(time_to_sleep)
        print(f"\t<data collection ended for {href_name} after "
              f"sleeping for {time_to_sleep} seconds>\n")
    return dict_of_dfs


tester = {'NYH ULSD-Heating Oil':
          r'https://www.cmegroup.com/trading/energy/refined-products/heating-oil.html'}

energy_dict = get_dict_of_dfs(urls)
energy_dict = get_dict_of_dfs(tester)
print(energy_dict.keys())
print(urls.keys())


for k, v in energy_dict.items():
    v.columns = [val[0] for val in v.columns]
    print(v.columns)

total_cols = []
for k, v in energy_dict.items():
    total_cols.append(v.columns)
    print(v.columns)
print(total_cols)

for i, _ in enumerate(total_cols):
    print(total_cols[i] == total_cols [i+1])

energy_dict.keys()



df_total = pd.DataFrame()
for _, v in energy_dict.items():
    v.drop(v.tail(1).index,inplace=True)
    df_total = df_total.append(v)
# df_total.shape

df_total.rename(columns={'metric_id': 'Metric',
                 'collected_tmstmp': 'Collected Timestamp'}, inplace=True)

df_total.columns

df_total.drop(
    columns=['Options', 'Unnamed: 12_level_0', 'Unnamed: 13_level_0'],
    inplace=True)

df_total.to_csv('Energy-Data Total v1.csv', index=False)














test_df = pd.DataFrame()
test_df.insert(0)
# test_href = urls['WTI']
# test_href_name = 'WTI'
# raw_html, href_name, current_tmstmp = \
#     html_from_javascript(test_href, test_href_name)

# df, prettified_soup = df_from_html(raw_html, href_name, current_tmstmp)

# save_raw_html(prettified_soup, test_href_name)

browser.get(url)
browser.get(test_other_url)

time.sleep = random.randint(4, 8)
html = browser.page_source
# soup = BeautifulSoup(html, 'lxml')
soup = BeautifulSoup(html, 'html.parser')

print(soup.prettify())

with open(r'C:\Users\GEM7318\Documents\Github\Store-Profitability\sample3'
           r'.txt', 'w', encoding='utf-8') as f:
    f.write(str(BeautifulSoup(raw_html, 'html.parser').prettify()))

print(

tables = soup.find_all('table')
print(tables[0].prettify())
len(tables)

# df = pd.read_html(str(tables))[0]
df2 = pd.read_html(str(tables))[0]
df.head()
tables[0]