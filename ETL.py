# Imports
import os
import pandas as pd
import calendar
from datetime import datetime as dt
import re
import FileHelper as fh
import numpy as np
from collections import Counter

# Creating dictionary of abbreviated month to month-index number
month_to_index = \
    {v: str(k).zfill(2) for k, v in enumerate(calendar.month_abbr) if k}
print(month_to_index)


def get_most_recently_modified_file():
    """
    Imports most recently modified raw output csv as a DataFrame
    :return: DataFrame
    """
    base_path = os.path.join(os.getcwd(), r'outputs_csv')
    full_paths = [os.path.join(base_path, val) for val in
                  os.listdir(base_path)]
    file_paths = [val for val in full_paths if os.path.isfile(val)]

    mod_file_dict = {os.path.getmtime(path): path for path in file_paths}
    most_recent_mod = \
        mod_file_dict[sorted(mod_file_dict.keys(), reverse=True)[0]]

    df = pd.read_csv(most_recent_mod)
    df.drop(df.head(1).index, inplace=True)
    print(f"Imported:\n\t\t{most_recent_mod}")

    return df


def standardize_cols(df):
    """
    Simple function to modify columns of df for easier modeling in Python.
    """
    df.columns = [col.lower().replace(' ', '_') for col in df.columns]
    df.columns = [col.lower().replace('_/_', '_') for col in df.columns]
    return df


# Importing df
df = get_most_recently_modified_file()

# df = pd.read_csv(r'C:\Users\GEM7318\Documents\Github\Energy-Scraping'
#                  r'\outputs_csv\2020-04-06 ~ Combined Output ~ v3.csv')
# df.drop(df.head(1).index, inplace=True)
# Standardizing columns
df = standardize_cols(df)

# Dropping unused columns
cols_to_drop = ['options', 'charts', 'last', 'change', 'open', 'high',
                'low', 'volume', 'hi_low_limit', 'unnamed:_12_level_0',
                'unnamed:_13_level_0', 'collected_timestamp']

df.drop(columns=cols_to_drop, inplace=True)


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
    splitter = val.split(' ')

    try:
        time_military = splitter[0]
        time_local = \
            dt.strptime(time_military, '%H:%M:%S').strftime('%I:%M %p')
        time_zone = splitter[1]
        date = f"{splitter[-1]}-{month_to_index.get(splitter[-2])}-{splitter[-3]}"

    except:
        time_military, time_local, time_zone, date = ('-', '-', '-', '_')

    to_explode = f"{date}~{time_military}~{time_local}~{time_zone}"

    return to_explode


def explode_col_by_func(df: pd.DataFrame, old_col: str, new_cols: list,
                        func: object = parse_last_updated,
                        drop_old_col: bool = True) -> pd.DataFrame:
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
    df[new_cols] = df[old_col].apply(func).str.split('~', expand=True)
    if drop_old_col:
        df.drop(columns=[old_col], inplace=True)
    else:
        pass

    return df


# Implementing above function
cols_to_explode = ['last_updated_date', 'last_updated_time_military',
                   'last_updated_time_local', 'last_updated_time_zone']

df = explode_col_by_func(df, 'updated', cols_to_explode, parse_last_updated)


# Dropping military time column pre-pivot
df.drop(columns=['last_updated_time_military'], inplace=True)

# Pivoting all columns in DataFrame
df_pivoted = df.pivot(index='month', columns='metric_id',
                      values=df.columns.tolist()[2:])

# Combining multi-index into single column index
df_pivoted.columns = [f"{col[0]}: {col[1]}" for col in df_pivoted.columns]

df_pivoted.index.name = 'month'
df_pivoted.reset_index(inplace=True)


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


def coalesce(df: pd.DataFrame, cols_to_coalesce: list,
             col_to_coalesce_into: str, drop_coalesced_cols: bool = True) \
            -> None:
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


# Coalescing many columns into one
cols_time_zone = [col for col in df_pivoted.columns if 
                  re.findall('.*_zone:', col) != []]

cols_collected_date = [col for col in df_pivoted.columns if re.findall(
    'collected_date: ', col) != []]

cols_updated_date = [col for col in df_pivoted.columns if re.findall(
    'last_updated_date: ', col) != []]

cols_local_time = [col for col in df_pivoted.columns if re.findall(
    '.*time_local:', col) != []]


coalesce_col_names = ['updated_time_zone', 'collected_date',
                      'updated_date', 'updated_time']
cols_to_coalesce = [cols_time_zone, cols_collected_date, 
                    cols_updated_date, cols_local_time]

for col_nm, cols in zip(coalesce_col_names, cols_to_coalesce):
    coalesce(df_pivoted, cols, col_nm)



def get_numeric_time_index(val):
    """
    Function to create numeric value for month and year combination (for
    sorting purposes)
    """
    # print(val)
    if re.findall('-', val):
        val1, val2 = val.split('-')
    else:
        val1, val2 = val.split(' ')

    len_val_dict = {len(val1): val1, len(val2): val2}
    list_of_lens = sorted(list(len_val_dict.keys()))
    # print(len_val_dict)
    month = len_val_dict.get(list_of_lens[0])
    year = len_val_dict.get(list_of_lens[-1])

    month_index = month_to_index.get(month.title())
    # print(f"Year: {year}, Month: {month}")
    # print(f"20{year}{month_index}")
    to_return = int(f"20{year}{month_index}")
    return to_return
# get_numeric_time_index('Feb-31')
# get_numeric_time_index('31-Feb')


df_pivoted.insert(1, 'month_rank',
                  df_pivoted.month.apply(get_numeric_time_index))
df_pivoted.sort_values('month_rank', inplace=True)
df_pivoted.reset_index(drop=True, inplace=True)
df_pivoted.index.name = 'Month Index'
df_pivoted.reset_index(inplace=True)

def floatify_cols(df, list_of_cols):
    """
    Function to convert all numeric metric columns to floats (replacing
    dashes with NaN values along the way)
    """
    for col in list_of_cols:
        df[col] = df[col].replace('-', np.nan)
        df[col] = df[col].apply(float)

    return None


metric_cols = [col for col in df_pivoted.columns if re.findall(':', col)]
floatify_cols(df_pivoted, metric_cols)
df_pivoted.dtypes


def prettify_col(col):
    """
    Quick function to prettyify column names for Excel export.
    """
    if not re.findall(':', col):
        col = col.replace('_', ' ').title()
    else:
        _, metric_val = col.split(': ')
        col = metric_val.upper()

    return col



df_pivoted.columns = [prettify_col(col) for col in df_pivoted.columns]
dfp2 = df_pivoted.copy()

df_pivoted.rename(columns={'NYH ULSD-HEATING OIL': 'NYH ULSD-Heating Oil',
                          'BRENT': 'Brent',
                          'GASOLINE-RBOB': 'Gasoline-RBOB',
                           'USGC-HSFO': 'USGC-HSFO (/bbl)'}, inplace=True)


df_pivoted['USGC-HSFO'] = df_pivoted['USGC-HSFO (/bbl)'] / 42




df_pivoted = df_pivoted[['Collected Date',
                        'Updated Date',
                        'Updated Time',
                        'Updated Time Zone',
                        'Month',
                        'Month Index',
                        'Brent',
                        'Gasoline-RBOB',
                        'NYH ULSD-Heating Oil',
                        'USGC-ULSD',
                        'USGC-HSFO',
                        'WTI',
                        'USGC-HSFO (/bbl)']]

df_pivoted.insert(0, 'Lookup-Key', df_pivoted['Collected Date'].astype(str)
                  + ' - ' + df_pivoted['Month Index'].astype(str))

df_pivoted.drop(columns=['Month Index'], inplace=True)

# df_pivoted['Month Rank'].min()


df_vertical = df_pivoted.copy()
# df_transposed = df_pivoted.transpose()

# from importlib import reload
# reload(cr)
# reload(fh)

file_nm2 = fh.get_file_name('etl_outputs_xlsx',
                           'CME Group Futures Price - Prior Settle',
                           is_etl=True)

# Deriving context tab contents
path_to_read = os.path.join(os.getcwd(), r'urls.csv')
dtl_df = pd.read_csv(path_to_read)

collected_date = list(Counter(df_pivoted['Collected Date']).keys())[0]
date_df = pd.DataFrame(data=['CME Group (Prior Settle)', collected_date],
                       index=['Source', 'Date'], columns=['Href'])
date_df = date_df.reset_index()
date_df.rename(columns={'index': 'Name'}, inplace=True)

dtl_df = pd.concat([date_df, dtl_df])


dfs = {'Data_Vertical': df_pivoted, 'Context': dtl_df}

date_fmrt = '-'.join(collected_date.split('/')[::-1])
print(date_fmrt)
file_nm = f"CME Group Futures Price - Prior Settle {date_fmrt}.xlsx"

path_to_write = os.path.join(os.getcwd(), 'etl_outputs_xlsx', file_nm)
path2 = os.path.join(os.getcwd(), 'etl_outputs_xlsx', file_nm2)
print(path_to_write)
print(path2)

writer = pd.ExcelWriter(path_to_write, 'xlsxwriter')
for sheetname, df in dfs.items():

    if sheetname == 'Context':
        df.to_excel(writer, sheet_name=sheetname, index=False, header=False)
    else:
        df.to_excel(writer, sheet_name=sheetname, index=False)

    worksheet = writer.sheets[sheetname]  # pull worksheet object

    for idx, col in enumerate(df):  # loop through all columns
        series = df[col]
        max_len = max((
            series.astype(str).map(len).max(),  # len of largest item
            len(str(series.name))  # len of column name/header
            )) + 1  # adding a little extra space
        worksheet.set_column(idx, idx, max_len)  # set column width
writer.save()
    

