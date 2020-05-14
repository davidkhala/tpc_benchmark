import config
import pandas as pd


def write_to_csv_old(db, test, dataset, desc, columns, data, kind):
    """ write data to csv file"""

    # load data frame
    df = pd.DataFrame(data, columns=columns)

    # generate filename
    f = (f'{config.fp_results}{config.sep}{db}_{test}_{kind}_times-' +
         f'{dataset}-{desc}-{str(pd.Timestamp.now())}.csv')

    # write to file
    df.to_csv(f, index=False)


def make_name(db, test, cid, kind, datasource, desc):
    """Make a name for query results DataFrames to be saved

    Parameters
    ----------
    db : str, data base name, either 'bq' or 'sf'
    test : str, test being done, either 'h' or 'ds'
    cid : str, config id, i.e. '02A' for the experiment config number
    kind : str, kind of record, either 'results' or 'times'
    datasource : str, dataset if bq or database if snowflake
    desc : str, description of experiment
    """

    f = (f'{config.fp_results}{config.sep}{db}_{test}_{cid}_{kind}-' +
         f'{datasource}-{desc}-{str(pd.Timestamp.now())}.csv')

    return f
