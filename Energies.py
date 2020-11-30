# Imports
from selenium import webdriver
from bs4 import BeautifulSoup
import pandas as pd
import os
import time
import random
from datetime import datetime


def read_and_shuffle_hrefs():
    """
    Reads in csv of names and hrefs and returns shuffled dictionary to
    traverse.
    """
    path_to_read = os.path.join(os.getcwd(), r"urls.csv")
    urls_df = pd.read_csv(path_to_read)

    urls = urls_df.set_index("Name")["Href"].to_dict()

    names = [val for val in urls.keys()]
    random.shuffle(names)
    shuffled_urls = {k: urls[k] for k in names}

    return shuffled_urls


# urls = read_and_shuffle_hrefs()
# print(urls.keys())


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
    browser.get(href)

    time_to_sleep = random.randint(lower, upper)
    print(
        f"\t<page opened - started sleeping for {time_to_sleep} before "
        f"downloading page data>"
    )
    time.sleep(time_to_sleep)

    html = browser.page_source
    current_tmstmp = str(datetime.today())
    print("\t<downloaded of page data completed>")

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
    soup = BeautifulSoup(html, "html.parser")
    soup_prettified = str(soup.prettify())

    html_tables = soup.find_all("table")
    df = pd.read_html(str(html_tables))[0]

    df.insert(0, "Metric ID", href_name)
    df["Collected Timestamp"] = current_tmstmp
    print("\t<parsed HTML into dataframe>")

    return df, soup_prettified


def get_file_name(folder_ext: str, file_name: str) -> str:
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
    current_date = str(datetime.today()).split(" ")[0]

    files = os.listdir(base_path)
    pre_existing_files = [
        file for file in files if file_name in file and current_date in file
    ]
    index_num = len(pre_existing_files) + 1

    file_ext = folder_ext.split("_")[-1]
    file_name = f"{current_date} ~ {file_name} ~ v{index_num}.{file_ext}"

    return file_name


# get_file_name('outputs_csv', 'Daily Total')
# get_file_name('_txt', 'Brent')


def save_raw_html(prettified_html, href_name, folder_ext="_txt"):
    """
    Accepts prettified string of raw HTML and writes out to local text file.
    :param folder_ext: Base folder name to save raw html in
    :param prettified_html: String of HTML
    :param href_name: Name of href from which the HTML was pulled (used in
    file name)
    :return: None
    """

    file_name = get_file_name(folder_ext, href_name)
    path_to_write = os.path.join(os.getcwd(), folder_ext, file_name)

    with open(path_to_write, "w", encoding="utf-8") as f:
        f.write(prettified_html)

    print("\t<raw html written to local text file>")

    return None


def get_dict_of_dfs(dict_of_hrefs, sleep_floor=45, sleep_ceiling=65):
    """
    Accepts dictionary of names: hrefs and returns a dictionary of DataFrames
    containing the scraped, parsed, and tabularized data
    :param dict_of_hrefs: Dictionary of names to hrefs
    :param sleep_floor: Lower bound of time to sleep between loading page
    and pulling the source data
    :param sleep_ceiling: Upper bound of time to sleep between loading page
    :return: Dictionary of dataframes
    """
    dict_of_dfs = {}
    for href_name, href in dict_of_hrefs.items():

        print(f"Scraping started for: {href_name}")

        raw_html, current_tmstmp = html_from_javascript(
            href, sleep_floor, sleep_ceiling
        )

        df, prettified_soup = df_from_html(raw_html, href_name, current_tmstmp)
        save_raw_html(prettified_soup, href_name)
        dict_of_dfs[href_name] = df

        time_to_sleep = random.randint(100, 350)
        time.sleep(time_to_sleep)
        print(
            f"\t<data collection ended for {href_name} after "
            f"sleeping for {time_to_sleep} seconds>\n"
        )

    return dict_of_dfs


for i in range(1, 4):

    print(f"<sleeping for {25} minutes...\n\n")
    time.sleep(1500)

    urls = read_and_shuffle_hrefs()

    # Instantiating browser instance
    browser = webdriver.Chrome(os.path.join(os.getcwd(), r"chromedriver.exe"))

    # Getting dictionary of DataFrames
    energy_dict = get_dict_of_dfs(urls)

    # Stripping multi-level column index down to a single index
    for k, v in energy_dict.items():
        v.columns = [val[0] for val in v.columns]
        # v.drop(
        #     columns=['Options', 'Unnamed: 12_level_0', 'Unnamed: 13_level_0'],
        #     inplace=True)
        print(v.columns)

    # Combining into a single DataFrame
    df_total = pd.DataFrame()
    for _, v in energy_dict.items():
        v.drop(v.tail(1).index, inplace=True)
        df_total = df_total.append(v)
    df_total.head()
    # df_total.shape

    # Saving locally
    df_total.to_csv(f"Energy-Data Total v{i}.csv", index=False)

    print(f"<-------------run #{i} completed------------->")
