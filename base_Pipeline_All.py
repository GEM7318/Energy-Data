
# Imports
import os

import ETL as etl
import FileHelper as fh
import time

from importlib import reload
reload(etl)
reload(fh)


outputs_dir = os.path.join(os.getcwd(), 'outputs_csv')

dates = fh.get_distinct_dates_from_dir(outputs_dir)
# dates

files = [fh.get_latest_file_for_date(outputs_dir, date) for date in dates]
# files

for file in files:
    etl.run_pipeline(file)
    print("<sleeping for 65 seconds before proceeding to next file>")
    time.sleep(65)

