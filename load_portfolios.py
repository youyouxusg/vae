import pandas as pd
from datetime import datetime

def load_managed_portfolios(filename, daily=True, drop_perc=1, omit_prefixes=None, keeponly=None):
    
    if omit_prefixes is None:
        omit_prefixes = []
    if keeponly is None:
        keeponly = []

    # Decide on the date format based on whether data is daily or not
    if daily:
        date_format = '%m/%d/%Y'
    else:
        date_format = '%m/%Y'
    
    # Read DATA from CSV and parse dates; used to use date_parser=lambda x: datetime.strptime(x, date_format)
    DATA = pd.read_csv(filename, parse_dates=["date"], dayfirst=False, date_format=date_format)
    
    # If keeponly is specified, filter columns based on it
    if keeponly:
        columns_to_keep = ['date', 'rme'] + keeponly
        DATA = DATA[columns_to_keep]
    else:
        # drop columns with certain prefixes
        for prefix in omit_prefixes:
            DATA.drop(columns=[col for col in DATA.columns if col.startswith(prefix)], inplace=True)
    
    # Drop columns with more than drop_perc percentage of missing values
    thresh = len(DATA) * drop_perc
    DATA.dropna(axis=1, thresh=thresh, inplace=True)

    # Drop rows with missing values (after the first two columns)
    DATA.dropna(subset=DATA.columns[2:], inplace=True)
    assert len(DATA) > 0.75 * len(DATA), 'More than 25% of obs. need to be dropped!'

    # Extract required values
    dates = DATA['date']
    mkt = DATA['rme']
    re = DATA.iloc[:, 3:]
    names = re.columns.tolist()

    # Return the values
    return dates, re, mkt, names#, DATA

def load_ff25(datapath, daily=True, t0=None, tN=None):
    # Set default values for t0 and tN if they are not provided
    if t0 is None:
        t0 = datetime.min
    if tN is None:
        tN = datetime.max

    # Decide on the file names based on whether data is daily or not
    if daily:
        ffact5 = 'F-F_Research_Data_Factors_daily.csv'
        ff25 = '25_Portfolios_5x5_Daily_average_value_weighted_returns_daily.csv'
    else:
        ffact5 = 'F-F_Research_Data_Factors.csv'
        ff25 = '25_Portfolios_5x5_average_value_weighted_returns_monthly.csv'

    # Read DATA from CSV
    DATA = pd.read_csv(datapath + ffact5, parse_dates=["Date"], dayfirst=False)
    
    # Filter rows based on date range
    DATA = DATA[(DATA['Date'] >= t0) & (DATA['Date'] <= tN)]
    
    # Read RET from CSV
    RET = pd.read_csv(datapath + ff25, parse_dates=["Date"], dayfirst=False)
    
    # Inner join of DATA and RET on 'Date' column
    DATA = pd.merge(DATA, RET, on='Date', how='inner')

    # Extract required columns and perform operations
    dates = DATA['Date']
    mkt = DATA['Mkt-RF'] / 100
    ret = DATA.iloc[:, 5:30].divide(100) - DATA['RF'] / 100
    labels = RET.columns[1:].tolist()

    return dates, ret, mkt, DATA, labels

def load_ff_anomalies(datapath, daily=True, t0=None, tN=None):
    # Setting default values for t0 and tN if they are not provided
    if t0 is None:
        t0 = datetime.min
    if tN is None:
        tN = datetime.max

    # Decide on the file names based on whether data is daily or not
    if daily:
        ffact5 = 'F-F_Research_Data_5_Factors_2x3_daily.csv'
        fmom = 'F-F_Momentum_Factor_daily.csv'
    else:
        ffact5 = 'F-F_Research_Data_5_Factors_2x3.csv'
        fmom = 'F-F_Momentum_Factor.csv'

    # Read DATA from CSV
    DATA = pd.read_csv(datapath + ffact5, parse_dates=["Date"], dayfirst=False)
    
    # Filter rows based on date range
    DATA = DATA[(DATA['Date'] >= t0) & (DATA['Date'] <= tN)]
    
    # Read MOM from CSV
    MOM = pd.read_csv(datapath + fmom, delimiter=",", parse_dates=["Date"], dayfirst=False)
    # remove additional blank spaces in column names
    MOM.columns = MOM.columns.str.strip()
    
    # Inner join of DATA and MOM on 'Date' column
    DATA = pd.merge(DATA, MOM, on='Date', how='inner')
    
    # Extract required columns and perform operations
    dates = DATA['Date']
    ret = DATA[['SMB', 'HML', 'Mom', 'RMW', 'CMA']] / 100
    mkt = DATA['Mkt-RF'] / 100
    
    # No de-market operation is done in the provided MATLAB function so omitting that

    return dates, ret, mkt, DATA

