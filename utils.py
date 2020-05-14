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



