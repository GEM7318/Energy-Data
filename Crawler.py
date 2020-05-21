
import FileHelper as fh

from bs4 import BeautifulSoup
from selenium import webdriver
import pandas as pd
import time
import random
from datetime import datetime

# TODO: Break Scroller into its own module


def get_list_of_directions(up_lower: int = 2, up_upper: int = 4,
                           down_lower: int = 0, down_upper: int = 2) \
                            -> list:
    """Generates n-length list of randomly-generated directions to scroll
    based on lower/upper bounds of times to scroll in each direction.
    Args:
        up_lower: Lower bound of number of times to scroll up
        up_upper: upper bound of times of times to scroll up
        down_lower: Lower bound of number of times to scroll down
        down_upper: upper bound of times of times to scroll down
    Returns:
        List of Up/Down directions to scroll
    """

    down_scroll = [f"Down-{val}"
                   for val in range(1, random.randint(up_lower, up_upper))]
    up_scroll = [f"Up-{other}"
                 for other in range(1, random.randint(down_lower,
                                                      down_upper) + 1)]

    all_scroll = up_scroll + down_scroll
    random.shuffle(all_scroll)
    all_directions = [val.split('-')[0] for val in all_scroll]

    return all_directions


def calc_random_scroll_amt(direction: str, screen_height: int = 1080,
                           lower_bound: float = 0.5,
                           upper_bound: float = 1.0) -> int:
    """
    Derives numeric value to scroll by based on a few parameters.
    :param direction: Up or Down direction to scroll
    :param screen_height: Height of screen
    :param lower_bound: Minimum percent of screen height you want to scroll
    :param upper_bound: Maximum percent of screen height you want to scroll
    :return: Randomly generated positive or negative integer to scroll based on
    the above parameters
    """
    sign_dict = {'Down': -1, 'Up': 1}

    rand_ratio = random.uniform(lower_bound, upper_bound)

    abs_scroll_amount = int(rand_ratio * screen_height)
    scroll_total = abs_scroll_amount * sign_dict.get(direction)

    print(f"\t\t<Scrolling {direction} by {abs_scroll_amount} pixels "
          f"based bounded-random-ratio of {round(rand_ratio, 2)}>")

    return scroll_total


def simulate_scrolling(browser: webdriver,
                       sleep_lower: int = 20, sleep_upper: int = 45) \
                        -> object:
    """
    Scrolls on a web page based on a list of Up/Down directions and
    :param browser: Webdriver browser object (pre-instantiated)
    :param sleep_lower: Lower bound of seconds to sleep for between scrolls
    :param sleep_upper: Upper bound of seconds to sleep for between scrolls
    :return: None
    """
    print("<Begin Scrolling Simluation>")
    total_scroll_height = \
        browser.execute_script("return document.body.scrollHeight")

    list_of_directions = get_list_of_directions()

    for i, direction in enumerate(list_of_directions, start=1):

        int_to_scroll = calc_random_scroll_amt(direction)
        current_position = browser.execute_script("return window.scrollY")
        next_position = current_position + int_to_scroll

        adj_needed = next_position > total_scroll_height or next_position <= 0
        if adj_needed:
            next_position = current_position + (int_to_scroll * -1)
        else:
            pass

        browser.execute_script(f"window.scrollTo({current_position},"
                               f" {next_position})")

        time_to_sleep = random.randint(sleep_lower, sleep_upper)
        print(f"\t\t{i} of {len(list_of_directions)} simulated "
              f"scrolls completed - sleeping for {time_to_sleep} seconds"
              f" before progressing")
        time.sleep(time_to_sleep)

    return None


def html_from_javascript(browser: object, href: str):
    """
    Retrieves html from web page given an href, delaying for javascript to
    fire and load data.
    :param browser: Webdriver browser object
    :param href: href of web page
    :return: Pre-soup HTML string, String of timestamp
    """
    browser.get(href)

    # time_to_sleep = random.randint(lower, upper)
    print(f"\t<page opened>")
    time.sleep(random.randint(3, 8))  # Sleeping a bit before scrolling
    simulate_scrolling(browser)

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
    current_date, current_tmstmp = current_tmstmp.split(' ')
    df['Collected Timestamp'] = current_tmstmp
    df['Collected Date'] = current_date
    print("\t<parsed HTML into dataframe>")

    return df, soup_prettified


def get_dict_of_dfs(dict_of_hrefs, browser,
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
            html_from_javascript(browser, href)

        df, prettified_soup = df_from_html(raw_html, href_name, current_tmstmp)
        fh.save_raw_file(prettified_soup, href_name, 'outputs_txt')

        dict_of_dfs[href_name] = df

        time_to_sleep = random.randint(minutes_page_sleep_floor*60,
                                       minutes_page_sleep_ceiling*60)
        print(f"\t<data collection ended for {href_name} - now sleeping for "
              f"sleeping for {time_to_sleep} seconds>\n")
        time.sleep(time_to_sleep)

    browser.close()
    return dict_of_dfs


# TODO: Add  to the end of get_dict_of_dfs() function to close
#   the browser once the job is finished