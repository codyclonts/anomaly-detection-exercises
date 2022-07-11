
# ignore warnings
import warnings
warnings.filterwarnings("ignore")

# Wrangling
import pandas as pd
import numpy as np

# Exploring
import scipy.stats as stats

# Visualizing

import matplotlib.pyplot as plt
import seaborn as sns

from env import get_db_url
import os
from sklearn.model_selection import train_test_split
import env






def get_log_data():
    '''
    This function pulls the log data from local csv if one exists. If not, this function creates log data, concatenates data from sql, and writes it into a csv
    '''
    filename = 'logs.csv'
    if os.path.isfile(filename):
        return pd.read_csv(filename, index_col = 0)
    else:  
        def parse_log_entry(entry):
            parts = entry.split()
            output = {}
            output['ip'] = parts[0]
            output['timestamp'] = parts[3][1:].replace(':', ' ', 1)
            output['request_method'] = parts[5][1:]
            output['request_path'] = parts[6]
            output['http_version'] = parts[7][:-1]
            output['status_code'] = parts[8]
            output['size'] = int(parts[9])
            output['user_agent'] = ' '.join(parts[11:]).replace('"', '')
            return pd.Series(output)

        url = f'mysql+pymysql://{env.user}:{env.password}@{env.host}/logs'
        df = pd.read_sql('SELECT * FROM api_access', url)
        # df = pd.concat([df.entry, df.entry.apply(parse_log_entry)], axis=1)
        df = df.entry.apply(parse_log_entry)
        df.to_csv(filename)
        return df
