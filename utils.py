import config
import pandas as pd


def write_to_csv(db, test, dataset, desc, columns, data, kind):
    """ write data to csv file"""

    # load data frame
    df = pd.DataFrame(data, columns=columns)

    # generate filename
    f = (f'{config.fp_results}{config.sep}{db}_{test}_{kind}_times-' +
         f'{dataset}-{desc}-{str(pd.Timestamp.now())}.csv')

    # write to file
    df.to_csv(f, index=False)


def result_namer(db, test, dataset, desc, kind):
    """Make a name for query results DataFrames to be saved"""

    f = (f'{config.fp_results}{config.sep}{db}_{test}_{kind}_results-' +
         f'{dataset}-{desc}-{str(pd.Timestamp.now())}.csv')

    return f
