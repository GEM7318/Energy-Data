
import FileHelper as fh

from bs4 import BeautifulSoup
import pandas as pd
import time
import random
from datetime import datetime



def html_from_javascript(browser: object, href: str,
                         lower: int = 20, upper: int = 30):
    """
    Retrieves html from web page given an href, delaying for javascript to
    fire and load data.
    :param browser: Webdriver browser object
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


def get_dict_of_dfs(dict_of_hrefs, browser, sleep_floor=45, sleep_ceiling=65,
                    minutes_page_sleep_floor=2, minutes_page_sleep_ceiling=5):
    """
    Accepts dictionary of names: hrefs and returns a dictionary of DataFrames
    containing the scraped, parsed, and tabularized data
    :param dict_of_hrefs: Dictionary of names to hrefs
    :param browser: Webdriver browser object
    :param sleep_floor: Lower bound of time to sleep between loading page
    and pulling the source data
    :param sleep_ceiling: Upper bound of time to sleep between loading page
    :param minutes_page_sleep_ceiling: Lower bound of time to sleep between
    pages (in minutes)
    :param minutes_page_sleep_floor: Upper bound of time to sleep between
    pages (in minutes)
    :return: Dictionary of DataFrames
    """
    dict_of_dfs = {}
    for href_name, href in dict_of_hrefs.items():

        print(f"Scraping started for: {href_name}")

        raw_html, current_tmstmp = \
            html_from_javascript(browser, href, sleep_floor, sleep_ceiling)

        df, prettified_soup = df_from_html(raw_html, href_name, current_tmstmp)
        fh.save_raw_file(prettified_soup, href_name, 'outputs_txt')

        dict_of_dfs[href_name] = df

        time_to_sleep = random.randint(minutes_page_sleep_floor*60,
                                       minutes_page_sleep_ceiling*60)
        print(f"\t<data collection ended for {href_name} - now sleeping for "
              f"sleeping for {time_to_sleep} seconds>\n")
        time.sleep(time_to_sleep)

    return dict_of_dfs


