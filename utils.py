import config
import time
import pandas as pd


def write_to_csv(prefix, test, scale, dataset, name, columns, data):
    """ write data to csv file"""

    # load data frame
    df = pd.DataFrame(data, columns=columns)

    # generate filename
    f = f'{config.fp_results}{config.sep}{prefix}_{test}_query_times-{dataset}-{name}-{time.strftime("%Y%m%d-%H%M%S")}.csv'

    # write to file
    df.to_csv(f, index=False)
