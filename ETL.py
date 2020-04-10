# Imports
import os
import pandas as pd
import calendar
from datetime import datetime as dt
import re


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
    print(f"Imported:\n\t\t{most_recent_mod}")

    return df


# Creating dictionary of abbreviated month to month-index number
month_to_index = \
    {v: str(k).zfill(2) for k, v in enumerate(calendar.month_abbr) if k}
print(month_to_index)

# Importing df
df = get_most_recently_modified_file()

# Dropping first row which was added from multi-column index
df.drop(df.head(1).index, inplace=True)

# Dropping 'unnammed' columns
df.drop(columns=[col for col in df.columns
                 if 'unnamed' in col.lower()
                 or 'options' in col.lower()
                 or 'charts' in col.lower()
                 ], inplace=True)

# Lower-casing all column names
df.columns = [col.lower().replace(' ', '_') for col in df.columns]
df.columns = [col.lower().replace('_/_', '_') for col in df.columns]


# List of preferred column order
# new_col_order = ['metric_id', 'month', 'prior_settle', 'last', 'open', 'high',
#                  'low', 'change', 'volume', 'hi_low_limit', 'updated',
#                  'collected_timestamp']

new_col_order = ['metric_id', 'month', 'prior_settle', 'updated',
                 'collected_timestamp']

# Changing column-order pre-pivot
df = df[new_col_order]

# Splitting 'high/low limit' into two different columns
# df[['limit_low', 'limit_high']] = \
#     df['hi_low_limit'].str.split('/', expand=True)

# Dropping old column
# df.drop(columns=['hi_low_limit'], inplace=True)


# Splitting 'updated' string into three different columns
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


# Implementing above function
cols_to_explode = ['last_updated_date', 'last_updated_time_military',
                   'last_updated_time_local', 'last_updated_time_zone']
df[cols_to_explode] = \
    df.updated.apply(parse_last_updated).str.split('~', expand=True)

# Dropping old column
df.drop(columns=['updated'], inplace=True)

# Parsing 'collected_date' into its own column from timestamp
df['collected_date'] = df.collected_timestamp.apply(lambda x: x.split(' ')[0])

# Dropping 'collected timestamp' column
df.drop(columns=['collected_timestamp'], inplace=True)

# Pivoting all columns in DataFrame
# test4 = df.pivot(index='month', columns='metric_id')
df_pivoted = df.pivot(index='month', columns='metric_id',
                      values=df.columns.tolist()[2:])

# Combining multi-index into single column index
df_pivoted.columns = [f"{col[0]}: {col[1]}" for col in df_pivoted.columns]


def coalesce(df1, cols_to_coalesce):
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

# Coalescing and dropping time zone
cols_to_coalesce = [col for col in df_pivoted.columns if
                    re.findall('.*_zone:', col) != []]

df_pivoted['last_updated_time_zone'] = coalesce(df_pivoted.copy(),
                                                cols_to_coalesce)

cols_to_drop = [col for col in df_pivoted.copy().columns if
                re.findall('.*_zone:', col) != []]

df_pivoted.drop(columns=cols_to_drop, inplace=True)

# Coalescing and dropping date
cols_to_coalesce = [col for col in df_pivoted.columns if 'collected_date' in
                    col]

df_pivoted['collected_date'] = coalesce(df_pivoted.copy(), cols_to_coalesce)

cols_to_drop = [col for col in df_pivoted.copy().columns if
                re.findall('collected_date: ', col) != []]

df_pivoted.drop(columns=cols_to_drop, inplace=True)

# Coalescing and dropping date
cols_to_coalesce = [col for col in df_pivoted.columns if
                    re.findall('.*time_local:', col) != []]

df_pivoted['last_updated_time_local'] = coalesce(df_pivoted.copy(),
                                            cols_to_coalesce)

cols_to_drop = [col for col in df_pivoted.copy().columns if
                re.findall('.*time_local:', col) != []]

df_pivoted.drop(columns=cols_to_drop, inplace=True)


# Stripping out index and ordering dataframe
df_pivoted.reset_index(inplace=True)


def get_numeric_time_index(val):
    """
    Function to create numeric value for month and year combination (for
    sorting purposes)
    """
    month, year = val.split(' ')
    month_to_index.get(val.split(' ')[0].title())
    to_return = int(f"{year}{month_to_index.get(month.title())}")
    return to_return

df_pivoted.insert(1, 'month_rank',
                  df_pivoted.month.apply(get_numeric_time_index))

df_pivoted.sort_values('month_rank', inplace=True)


def quick_col_reformat(col):
    if not re.findall(':', col):
        col = col.replace('_', ' ').title()
    else:
        first, last = col.split(': ')
        first = first.replace('_', ' ').title()
        col = f"{first}: {last}"

    return col


df_pivoted.columns = [quick_col_reformat(col) for col in df_pivoted.columns]

import FileHelper as fh

file_nm = fh.get_file_name('etl_outputs_csv', 'Cleansed Output')

df_pivoted = df_pivoted[['Collected Date',
                        'Last Updated Time Local',
                        'Last Updated Time Zone',
                        'Month',
                        'Month Rank',
                        'Prior Settle: Brent',
                        'Prior Settle: Gasoline-RBOB',
                        'Prior Settle: NYH ULSD-Heating Oil',
                        'Prior Settle: USGC-ULSD',
                        'Prior Settle: USGE-HSFO',
                        'Prior Settle: WTI']]


df_pivoted.to_csv(file_nm, index=False)




# df_pivoted.applymap(str.replace('+', ''))
# for col in df_pivoted.columns:
#     try:
#         df_pivoted[col] = df_pivoted[col].apply(
#             lambda x: str(x).replace('+', ''))
#         print(f"{col}")
#     except:
#         pass

# df_pivoted.loc['DEC 2020', 'change: Gasoline-RBOB']