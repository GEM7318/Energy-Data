
from selenium import webdriver
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
from datetime import datetime
import os


def html_from_javascript(href: str, lower: int = 20, upper: int = 30):
    """
    Retrieves html from web page given an href, delaying for javascript to
    fire and load data.
    :return:
    :param lower: Lower bound of seconds to sleep for
    :param upper: Upper bound of seconds to sleep for
    :param href: href of web page
    :return: string, string
    """
    browser = webdriver.Chrome(os.path.join(os.getcwd(), r'chromedriver.exe'))
    browser.get(href)

    time_to_sleep = random.randint(lower, upper)
    print(f"\t<page opened - started sleeping for {time_to_sleep} before "
          f"downloading page data>")
    time.sleep(time_to_sleep)

    html = browser.page_source
    current_tmstmp = str(datetime.today())
    print('\t<downloaded of page data completed>')

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

    df.insert(0, 'Metric ID', href_name)
    df['Collected Timestamp'] = current_tmstmp
    print("\t<parsed HTML into dataframe>")

    return df, soup_prettified

