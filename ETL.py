# Imports
import os
import pandas as pd
import calendar
from datetime import datetime as dt
import re
import numpy as np
from collections import Counter

# Creating dictionary of abbreviated month to month-index number
month_to_index = {v: str(k).zfill(2) for k, v in enumerate(calendar.month_abbr) if k}


def standardize_excel_date_str(val):
    """
    Quick & dirty function to convert the Excel-reformatted M/D/YYYY to a
    YYYY-MM-DD style string.
    """
    if re.findall("/", val):
        month, day, year = val.split("/")
    else:
        year, month, day = val.split("-")

    reformatted_date = (
        f"{str(year).zfill(4)}-{str(month).zfill(2)}" f"-{str(day).zfill(2)}"
    )

    return reformatted_date


#
#
# tester = pd.read_csv(path_str)
# tester = pd.read_csv(
#     r'C:\Users\GEM7318\Documents\Github\Energy-Scraping\outputs_csv\2020-04-12 ~ Combined Output ~ v1.csv')
# test_date = list(tester['Collected Date'])[2]
# testyear, testmonth, testday = test_date.split('-')
# test_date.split('/')
# standardize_excel_date_str(test_date)


# standardize_excel_date_str('4/12/2020')


def read_csv_from_path(full_path: str):
    """
    Reads in combined csv from single scraping run output based on a
    directory path and performs some basic cleanup.
    :param full_path: Path where CSV is stored
    :return: pd.DataFrame with slight modifications performed
    """
    # full_path = r'C:\Users\GEM7318\Documents\Github\Energy-Scraping' \
    #            r'\outputs_csv\2020-04-23 ~ Combined Output ~ v1.csv'
    df = pd.read_csv(full_path)
    # df.drop(df.head(1).index, inplace=True)
    df.drop(index=0, inplace=True)

    df.columns = [col.lower().replace(" ", "_") for col in df.columns]
    df.columns = [col.lower().replace("_/_", "_") for col in df.columns]

    cols_to_drop = [
        "options",
        "charts",
        "last",
        "change",
        "open",
        "high",
        "low",
        "volume",
        "hi_low_limit",
        "collected_timestamp",
    ]
    unnamed_cols_to_drop = [col for col in df.columns if "unnamed" in col.lower()]
    cols_to_drop = cols_to_drop + unnamed_cols_to_drop
    for col in cols_to_drop:
        try:
            df.drop(columns=[col], inplace=True)
        except:
            print(f"Could not drop column {col} from file")
    # df.drop(columns=cols_to_drop, inplace=True)

    df.collected_date = df.collected_date.apply(standardize_excel_date_str)

    df = df[["metric_id", "month", "prior_settle", "updated", "collected_date"]]

    return df


# path_str2 = r'C:\Users\GEM7318\Documents\Github\Energy-Scraping' \
#            r'\outputs_csv\2020-04-21 ~ Combined Output ~ v1.csv'
#
# path_str = r'C:\Users\GEM7318\Documents\Github\Energy-Scraping' \
#            r'\outputs_csv\2020-04-23 ~ Combined Output ~ v1.csv'
# read_csv_from_path(path_str)
# TODO: Modularize the above two functions (ETL-reader)

# df1 = read_csv_from_path(path_str2)
# df2 = read_csv_from_path(path_str)
# df1.columns
# df2.columns


def parse_last_updated(val):
    """
    Custom function to split the 'updated' time string into date,
    24hr/military time, local time, and local time zone.
    Returning tilda-delimited string instead of a tuple since Panda's
    column-wise explosion works best when splitting on a single delimiter as
    opposed to unpacking the Tuple in a more pythonic way
    :param val: String to parse
    :return: Tilda-delimited string to split/explode into multiple columns
    """
    splitter = val.split(" ")

    try:
        time_military = splitter[0]
        time_local = dt.strptime(time_military, "%H:%M:%S").strftime("%I:%M %p")
        time_zone = splitter[1]
        date = f"{splitter[-1]}-{month_to_index.get(splitter[-2])}-{splitter[-3]}"

    except:
        time_military, time_local, time_zone, date = ("-", "-", "-", "_")

    to_explode = f"{date}~{time_military}~{time_local}~{time_zone}"

    return to_explode


def explode_col_by_func(
    df: pd.DataFrame,
    old_col: str,
    new_cols: list,
    func: object = parse_last_updated,
    drop_old_col: bool = True,
) -> pd.DataFrame:
    """
    Function to explode a single column into multiple columns based on
    custom function that returns a tilda delimited string of new column values.
    :param df: DataFrame to perform operation on
    :param old_col: Name of old column
    :param new_cols: List of new column names
    :param func: Function to apply to old column
    :param drop_old_col: Boolean value of whether to drop old column value
    or not
    :return: DataFrame with explosion operation performed
    """
    df[new_cols] = df[old_col].apply(func).str.split("~", expand=True)
    if drop_old_col:
        df.drop(columns=[old_col], inplace=True)
    else:
        pass

    return df


# TODO: Modularize the above two functions (column-exploder)


def get_coalesced_col(df1, cols_to_coalesce):
    """
    Quick & dirty custom function to SQL-style coalesce multiple columns into
    a single field.
    :param df1:
    :param cols_to_coalesce:
    :return:
    """
    i = iter(cols_to_coalesce)
    column_name = next(i)
    coalesced = df1[column_name]
    for column_name in i:
        coalesced = coalesced.fillna(df1[column_name])
    return coalesced


def coalesce(
    df: pd.DataFrame,
    cols_to_coalesce: list,
    col_to_coalesce_into: str,
    drop_coalesced_cols: bool = True,
) -> None:
    """
    Function to in-place coalesce multiple columns of a DataFrame into a
    single column.
    :param df: DataFrame to perform operation on
    :param cols_to_coalesce: Columns to coalesce
    :param col_to_coalesce_into: Name of column to coalesce into
    :param drop_coalesced_cols: Boolean value indicating whether or not to
    coalesce columns
    """
    df[col_to_coalesce_into] = get_coalesced_col(df.copy(), cols_to_coalesce)

    if drop_coalesced_cols:
        df.drop(columns=cols_to_coalesce, inplace=True)
    else:
        pass

    return None


def coalesce_multiple(
    df: pd.DataFrame,
    col_patterns_to_coalesce: list,
    col_nms_to_coalesce_into: list,
    drop_coalesced_cols: bool = True,
) -> None:
    """
    Function to in-place coalesce multiple sets of columns within a
    DataFrame into a single column per set.
    :param df: DataFrame to perform operation on
    :param col_patterns_to_coalesce: N-length list of regex patterns to
    identify different sets of columns
    :param col_nms_to_coalesce_into: N-length List of column names to store
    results of each coalescion in
    :param drop_coalesced_cols: Boolean value indicating whether or not to
    columns that have been coalesced
    """
    sets_of_cols = []
    for patt in col_patterns_to_coalesce:
        cols = [col for col in df.columns if re.findall(patt, col) != []]
        sets_of_cols.append(cols)

    for cols, col_nm in zip(sets_of_cols, col_nms_to_coalesce_into):
        coalesce(df, cols, col_nm, drop_coalesced_cols)

    return None


# TODO: Modularize the above three functions (df-coalescer)


def get_numeric_time_index(val):
    """
    Function to create numeric value for month and year combination (for
    sorting purposes)
    """
    if re.findall("-", val):
        val1, val2 = val.split("-")
    else:
        val1, val2 = val.split(" ")

    if val1.isnumeric():
        year, month = str(val1)[-2:], val2
    else:
        month, year = val1, str(val2)[-2:]

    month_index = month_to_index.get(month.title())
    to_return = int(f"20{year}{month_index}")

    return to_return


# print(month_to_index.keys())
# get_numeric_time_index('Feb-31')
# get_numeric_time_index('31-Feb')
# get_numeric_time_index('FEB-2031')
# get_numeric_time_index('2031-FEB')
# get_numeric_time_index('Jan 2020')
# get_numeric_time_index('Jan 2030')
# # test = [1,2,3]
# # test[::-2]
#
# tester = 'JAN 2030'
# val1, val2 = tester.split(' ')
# print(val1, val2)
# val1.isnumeric()
# month, year = val1, str(val2)[-2:]
# str(val2)
# month
# year
# problem is in both month_index and hash functions


def get_month_hash_and_sort(
    df: pd.DataFrame,
    hash_parent: str = "Month Index",
    hash_child: str = "collected_date",
    name_of_hash: str = "Lookup-Key",
    drop_month_index: bool = True,
) -> None:
    """
    Creates numeric index for months and a unique identifier for a given
    futures month based on a collected date
    :param df: DataFrame to perform operation on
    :param hash_parent: First level needed for unique relationship
    :param hash_child: Second level needed for unique relationship
    :param name_of_hash: Name of hashed field to user in DataFrame
    :param drop_month_index: Boolean value indication whether or not to drop
    the numeric index for month once it has been embedded into the unique field
    """
    df.insert(1, "month_rank", df.month.apply(get_numeric_time_index))

    df.sort_values("month_rank", inplace=True)
    df.reset_index(drop=True, inplace=True)
    df.index.name = hash_parent
    df.reset_index(inplace=True)

    df.insert(
        0,
        name_of_hash,
        df[hash_child].astype(str) + " - " + df[hash_parent].astype(str),
    )

    if drop_month_index:
        df.drop(columns=[hash_parent], inplace=True)

    return None


# TODO: Fix such that the sorting still works for recent/forward looking files

# TODO: Modularize the above two functions (hash-sorter)

# df = read_csv_from_path(r'C:\Users\GEM7318\Documents\Github\Energy-Scraping'
#                  r'\outputs_csv\2020-04-12 ~ Combined Output ~ v2.csv')
# get_month_hash_and_sort(df)


def floatify_cols(df, list_of_cols):
    """
    Function to convert all numeric metric columns to floats (replacing
    dashes with NaN values along the way)
    """
    for col in list_of_cols:
        df[col] = df[col].replace("-", np.nan)
        df[col] = df[col].apply(float)

    return None


def prettify_col(col):
    """
    Quick function to prettyify column names for Excel export.
    """
    if not re.findall(":", col):
        col = col.replace("_", " ").title()
    else:
        _, metric_val = col.split(": ")
        col = metric_val.upper()

    return col


def prettify_cols_for_export(df: pd.DataFrame):
    """
    Implements prettify_col() function and does in-place manual renaming of
    some edge cases.
    """
    df.columns = [prettify_col(col) for col in df.columns]
    df.rename(
        columns={
            "NYH ULSD-HEATING OIL": "NYH ULSD-Heating Oil",
            "BRENT": "Brent",
            "GASOLINE-RBOB": "Gasoline-RBOB",
            "USGC-HSFO": "USGC-HSFO (/bbl)",
        },
        inplace=True,
    )
    return None


# TODO: Modularize the above three functions (df-export-prepper)


def get_context_and_date_for_data(df_data: pd.DataFrame) -> tuple:
    """
    Generates the DataFrame for 'Context' tab of ETL output as well as
    extracts and re-formats the 'collected date'.
    :param df_data: Pivoted DataFrame for primary output tab
    :return: DataFrame to populate 'Context' tab and the formatted date for
    file name
    """
    path_to_urls = os.path.join(os.getcwd(), r"urls.csv")
    context_df = pd.read_csv(path_to_urls)

    date_frmt = list(Counter(df_data["Collected Date"]).keys())[0]

    source_date_df = pd.DataFrame(
        data=["CME Group (Prior Settle)", date_frmt],
        index=["Source", "Date"],
        columns=["Href"],
    )
    source_date_df = source_date_df.reset_index()
    source_date_df.rename(columns={"index": "Name"}, inplace=True)

    context_df = pd.concat([source_date_df, context_df])

    return context_df, date_frmt


def fancy_excel_writer(path_to_write: str, dict_of_dfs: dict) -> None:
    """
    Function to write out to a .xlsx file based on a path to write to and a
    dict of DataFrames.
    :param path_to_write: Full file path to write to
    :param dict_of_dfs: Dictionary of DataFrames following the convention
    Tab Name: Data for Tab
    """
    writer = pd.ExcelWriter(path_to_write, "xlsxwriter")
    for sheetname, df in dict_of_dfs.items():

        if sheetname == "Context":
            df.to_excel(writer, sheet_name=sheetname, index=False, header=False)
        else:
            df.to_excel(writer, sheet_name=sheetname, index=False)

        worksheet = writer.sheets[sheetname]  # pull worksheet object

        for idx, col in enumerate(df):  # loop through all columns
            series = df[col]
            max_len = (
                max(
                    (
                        series.astype(str).map(len).max(),  # len of largest item
                        len(str(series.name)),  # len of column name/header
                    )
                )
                + 1
            )  # adding a little extra space
            worksheet.set_column(idx, idx, max_len)  # set column width
    writer.save()

    return None


# TODO: Modularize the above two functions (Excel-Handler)


def run_pipeline(path_str):
    print(f"Pipeline Started for:\n\t{path_str}")

    # =========================================================================
    # Importing and lightly cleaning up from scraping CSV output
    # path_str = r'C:\Users\GEM7318\Documents\Github\Energy-Scraping' \
    #            r'\outputs_csv\2020-04-23 ~ Combined Output ~ v1.csv'
    # path_str = r'C:\Users\GEM7318\Documents\Github\Energy-Scraping' \
    #            r'\outputs_csv\2020-04-21 ~ Combined Output ~ v1.csv'
    df = read_csv_from_path(path_str)

    # [col for col in df.columns if 'collected' in col]
    # type(df.iloc[2])
    # df.shape

    # =========================================================================
    # Splitting components within 'last updated' column into their own fields
    cols_to_explode = [
        "last_updated_date",
        "last_updated_time_military",
        "last_updated_time_local",
        "last_updated_time_zone",
    ]

    df = explode_col_by_func(df, "updated", cols_to_explode, parse_last_updated)

    df.drop(columns=["last_updated_time_military"], inplace=True)

    # =========================================================================
    # Pivoting DataFrame based on 'metric_id' column
    df_pivoted = df.pivot(
        index="month", columns="metric_id", values=df.columns.tolist()[2:]
    )

    # Combining multi-index into single column index
    df_pivoted.columns = [f"{col[0]}: {col[1]}" for col in df_pivoted.columns]

    df_pivoted.index.name = "month"
    df_pivoted.reset_index(inplace=True)
    df_pivoted.drop(
        columns=[col for col in df_pivoted.columns if "month:" in col], inplace=True
    )

    dfp1 = df_pivoted.copy()
    dfp2 = df_pivoted.copy()
    # dfp1.shape
    # dfp2.shape
    # [col for col in dfp2.columns if 'collected' in col]
    # [col for col in dfp1.columns if 'collected' in col]
    # [col for col in dfp2.columns]
    for col in dfp1.columns:
        if col in list(dfp2.columns):
            pass
        else:
            print(col)
    # =========================================================================
    # Coalescing multiple sets of columns into individual fields
    coalesced_col_nms = [
        "updated_time_zone",
        "collected_date",
        "updated_date",
        "updated_time",
    ]

    patterns_to_find = [
        ".*_zone:",
        "collected_date: ",
        "last_updated_date: ",
        ".*time_local:",
    ]

    coalesce_multiple(df_pivoted, patterns_to_find, coalesced_col_nms)

    # =========================================================================
    # Sorting on month/year and creating Lookup-ID Column
    get_month_hash_and_sort(df_pivoted)

    # =========================================================================
    # Cleansing & converting numeric columns to floats pre-export
    metric_cols = [col for col in df_pivoted.columns if re.findall(":", col)]
    floatify_cols(df_pivoted, metric_cols)

    # =========================================================================
    # Renaming column names pre-export
    prettify_cols_for_export(df_pivoted)

    # =========================================================================
    # Converting USGC-HSFO column to a per gallon like other metrics
    df_pivoted["USGC-HSFO"] = df_pivoted["USGC-HSFO (/bbl)"] / 42

    # =========================================================================
    # Re-ordering DataFrame based on standards stored externally
    # path_to_col_order = os.path.join(os.getcwd(), 'ETL_col_order.csv')
    path_to_col_order = os.path.join(os.getcwd(), "ETL_Output_Template.xlsx")

    df_cols = pd.read_excel(path_to_col_order)
    col_names_for_export = df_cols.columns.tolist()

    df_pivoted = df_pivoted[col_names_for_export]

    # =========================================================================
    # Generating DataFrame for 'Context' tab and getting date for file name
    context_df, date_frmt = get_context_and_date_for_data(df_pivoted)

    # =========================================================================
    # Creating dict of Tab_Name: Associated DataFrame for Excel writer
    dfs = {"Data_Vertical": df_pivoted, "Context": context_df}

    # =========================================================================
    # Creating file name and path to write data to
    file_nm = f"CME Group Futures Price - Prior Settle {date_frmt}.xlsx"
    path_to_write = os.path.join(os.getcwd(), "etl_outputs_xlsx", file_nm)
    path_to_write_2 = os.path.join(r"D:\Dropbox\1 - CME Group Futures Files", file_nm)

    # =========================================================================
    # Writing out to .xlsx
    fancy_excel_writer(path_to_write, dfs)
    fancy_excel_writer(path_to_write_2, dfs)
    print(f"Pipeline Completed for:\n\t{path_str}\n")
    return None


# path_to_csv = r'C:\Users\GEM7318\Documents\Github\Energy-Scraping' \
#               r'\outputs_csv\2020-04-06 ~ Combined Output ~ v3.csv'

# run_pipeline(path_to_csv)
