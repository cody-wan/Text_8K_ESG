import pandas as pd
import numpy as np
import logging
import requests
import json
import statsmodels.api as sm
from pandas.tseries.offsets import BDay
from datetime import datetime

logging.basicConfig(filename="logs/generate_price_history/log.txt",
                    filemode='a',
                    level=logging.INFO,
                    format='%(levelname)s: %(asctime)s - %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S')

# global variables
NUM_DAYS_IN_YEAR = 252


def get_NAICS_code_sector_name():
    # generate pair of NAICS sector code and name
    with open("data/industry_classification_and_portfolio/NACIS_sectors.json", "r") as f:
        NAICS_sectors = json.load(f)
    NACIS_sector_code = list({key: key.split(",") for key in NAICS_sectors}.values())
    NACIS_sector_name = list(NAICS_sectors.values())
    return NACIS_sector_code, NACIS_sector_name


def add_to_return_stats(index, df, return_stats):
    """
    add df, a pandas DataFrame with a single column, to return_stats
    assuming df's index has the form: 'CIK'_'date -1'_'date 0'
    """
    if df is not None:
        df.index = df.index.str.split("_", expand=True)
        col_name = df.columns[0]
        return_stats.insert(index, col_name, df[col_name])
    return return_stats


def get_earnings_call_date(db, CIK_TICKER_mapping, BUY_SIGNAL):
    """
        get the date of earnings announcement date from wrds/ibes.'statsum_epsus':
            take values of -1, 0, 1 if earnings is annouced on day -1, 0, 1 relative to event day; np.nan if not
    """
    # logging.info("START - fetching earnings call date")
    master_EA_date_dict = dict()
    ERROR_CIK = list()
    for CIK in BUY_SIGNAL:
        ticker = CIK_TICKER_mapping[CIK]
        event_dates = BUY_SIGNAL[CIK]
        try:
            all_EA_dates = db.raw_sql(f"SELECT anndats_act FROM ibes.statsum_epsus "
                                      f"WHERE oftic = '{ticker}'")['anndats_act'].dropna().unique().tolist()
        except Exception as e:
            logging.exception(e)
            ERROR_CIK.append(CIK)
            continue
        for date_start in event_dates:
            date_start = pd.to_datetime(date_start)
            date_prior = pd.to_datetime(date_start) - BDay(1)
            date_after = pd.to_datetime(date_start) + BDay(1)

            if date_start in all_EA_dates:
                flag = 0
            elif date_prior in all_EA_dates:
                flag = -1
            elif date_after in all_EA_dates:
                flag = 1
            else:
                flag = np.nan
            # append to master variable
            master_EA_date_dict[f"{CIK}_{date_prior.strftime('%Y-%m-%d')}_{date_start.strftime('%Y-%m-%d')}"] = [flag]

    # dump to dataframe
    res_EA_date_df = pd.DataFrame.from_dict(master_EA_date_dict, orient='columns').T
    # print to console number of CIKs earnings announcement history successfully accessed
    print(f"{len(res_EA_date_df)}/{sum([len(BUY_SIGNAL[CIK]) for CIK in BUY_SIGNAL])} earnings call date checked")

    # leave dataframe as None if empty
    if res_EA_date_df.empty:
        res_EA_date_df = None
    else:
        res_EA_date_df.columns = ['EA']

    # logging.info("END - fetching earnings call date")

    return res_EA_date_df, ERROR_CIK


def get_price_history_yahoo_finance(CIK_TICKER_mapping, BUY_SIGNAL, HOLDING_PERIOD, frequency='1d',
                                    begin_time=1546322400,
                                    finish_time=int(datetime.now().timestamp())):
    """
        read price data from yahoo finance
        args:
            TICKER
            frequency: '1d' or '1wk' or '1mo'
            begin_time: int, epoch time, default is 1/1/2019
            finish_time: int, epoch time, default is current time
        returns:
            dataframe that has price data, same as from generate_price_history(), which will be concatenated together
    """
    # logging.info(f"START yahoo finance")

    # initialize dictionary to store daily price for each event
    master_daily_price_dict = dict()
    master_vol_pct_dict = dict()
    # initialize variable to store event that require price data beyond 2019-12-31
    ERROR_event = dict()

    # iterate through every CIK
    for CIK in BUY_SIGNAL:
        # get permno and event date
        ticker = CIK_TICKER_mapping[CIK]
        event_dates = BUY_SIGNAL[CIK]
        # iterate through every event date
        for date_start in event_dates:

            date_prior = (pd.to_datetime(date_start) - BDay(1)).strftime("%Y-%m-%d")
            date_after = (pd.to_datetime(date_start) + BDay(1)).strftime("%Y-%m-%d")

            # define endpoint for making web request
            endpoint = fr"https://query1.finance.yahoo.com/v7/finance/download/{ticker}"
            param_dict = {
                'period1': begin_time,
                'period2': finish_time,
                'interval': frequency,
                'events': 'history',
                'includeAdjustedClose': 'true'
            }
            response = requests.get(url=endpoint, params=param_dict)
            if response.status_code == 200:
                df = pd.read_csv(response.url)
            else:
                ERROR_event[CIK] = ERROR_event.get(CIK, []) + [date_start]
                logging.error(f"broken url for {ticker}")
                continue

            df = df[['Date', 'Adj Close', 'Volume']].astype({'Date': np.datetime64}).set_index('Date')
            daily_price = df['Adj Close'].loc[date_prior:].tolist()[:HOLDING_PERIOD]

            # log CIK, date range with no price history, and skip to next one
            if len(daily_price) != HOLDING_PERIOD:
                logging.info(
                    f"Insufficient Price History from Yahoo Finance - CIK={CIK} - ticker={ticker} - {date_start}")
                continue

            # append to master variables
            master_daily_price_dict[f"{CIK}_{date_prior}_{date_start}"] = daily_price
            daily_vol = df['Volume'].loc[:date_after]
            master_vol_pct_dict[f"{CIK}_{date_prior}_{date_start}"] = [
                daily_vol.iloc[-3:].mean() / daily_vol[:-3].mean()]

    # first price in res_price_df is end of day before signal event date
    res_price_df = pd.DataFrame.from_dict(master_daily_price_dict, orient='columns')
    res_vol_df = pd.DataFrame.from_dict(master_vol_pct_dict, orient='columns').T
    if res_vol_df.empty:
        res_vol_df = None
    else:
        res_vol_df.columns = ['vol']
    print(f"{len(res_price_df.columns)}/{sum([len(BUY_SIGNAL[CIK]) for CIK in BUY_SIGNAL])} price history generated "
          f"from yahoo finance")
    # logging.info(f"END yahoo finance")

    return res_price_df, res_vol_df, ERROR_event


def get_price_history(db, CIK_PERMNO_mapping, BUY_SIGNAL, HOLDING_PERIOD, TRACE_BACK_PERIOD=120):
    """
        generate cumulative returns of each event from BUY_SIGNAL for (roughy) HOLDING_PERIOD many business days
        args:
            CIK_PERMNO_mapping: dictionary {CIK: set(PERMNO1, PERMNO2)}
            BUY_SIGNAL: dict: {'CIK': ['event date 1', 'event date2']}; e.g. {1177609: ['2012-07-26', '2013-08-22']}
            HOLDING_PERIOD: int; e.g. 30*3 (roughly 3 months)
        returns:
            res, pandas DataFrame:
                index: integer from 0 to HOLDING_PERIOD
                    this includes price data of the day prior to event + price data during HOLDING_PERIOD
                column: label of each event "{CIK}_{date_start}"
            ERROR_event:
                list of event that requires more data than what wrds provides
        NOTE: 
            price history data from wrds is only available through 2019-12-31
            
    """
    # logging.info(f"START wrds/crsp")

    # initialize dictionary to store daily price for each event 
    master_daily_price_dict = dict()
    master_vol_pct_dict = dict()
    # initialize variable to store event that require price data beyond 2019-12-31
    ERROR_event = dict()
    # iterate through every CIK
    for CIK in BUY_SIGNAL:
        # get permno and event date
        permno_set = CIK_PERMNO_mapping[CIK]
        event_dates = BUY_SIGNAL[CIK]
        # iterate through every event date
        for date_start in event_dates:

            # # "datetime_start" contains info through hour/minute/second
            # date_start = datetime_start

            # the day prior to date_start (we're interested in if there's any price jump due to event)
            date_prior = (pd.to_datetime(date_start) - BDay(1)).strftime("%Y-%m-%d")
            date_after = (pd.to_datetime(date_start) + BDay(1)).strftime("%Y-%m-%d")
            # go backing "TRACE_BACK_PERIOD" # of days for computing average trading volume and compare that with
            # event day trading volume "+5" is to get additional days (in case it falls short due to difference from
            # actual price dates from crsp) when fetching from crsp, # dates will be min(TRACE_BACK_PERIOD,
            # event_day - date_trace_back_start)
            date_trace_back_start = pd.to_datetime(date_start) - BDay(TRACE_BACK_PERIOD + 5)
            # therefore, # of price data we need = TRACE_BACK_PERIOD + HOLDING_PERIOD
            TOTAL_PRICE_PERIOD = TRACE_BACK_PERIOD + HOLDING_PERIOD

            # computes date_finish, for skipping this event if not available in crsp
            date_finish = pd.to_datetime(date_start) + BDay(HOLDING_PERIOD)
            if date_finish > pd.to_datetime('2019-12-31'):
                ERROR_event[CIK] = ERROR_event.get(CIK, []) + [date_start]
                continue
            else:
                date_finish = date_finish.strftime('%Y-%m-%d')

            # since there could be multiple permno corresponding to a CIK
            # iterate through the set of all historical permno
            # use the one that gives sufficient price history for the period we want
            for permno in permno_set:
                # read historical daily price; NOTE: wrds data is only available through 2019-12-31
                # we call wrds twice for daily price and vol separatly; this ensures the number of date points
                # for price and vol are strictly the same as desired. there might be a more efficient way with 
                # advanced SQL syntax (or not)
                daily_price = db.raw_sql(
                    f"SELECT date, prc, cfacpr FROM crsp.dsf WHERE permno={permno} AND "
                    f"cast(date as DATE) >= '{date_prior}' LIMIT {HOLDING_PERIOD}").astype({'date': np.datetime64})
                daily_vol = db.raw_sql(
                    f"SELECT date, vol, cfacshr FROM crsp.dsf WHERE permno={permno} AND "
                    f"cast(date as DATE) >= '{date_trace_back_start}' AND cast(date as DATE) <= '{date_after}' "
                    f"LIMIT {TRACE_BACK_PERIOD}").astype({'date': np.datetime64})
                if len(daily_price) == HOLDING_PERIOD and len(daily_vol) == TRACE_BACK_PERIOD:
                    break

            # log CIK, date range with no price history, and skip to next one
            if len(daily_price.dropna()) < HOLDING_PERIOD:
                if len(daily_price) == 0:
                    logging.info(
                        f"NO Price History - CIK={CIK} - PERMNO={permno_set} - date_range='{date_start}, {date_finish}''")
                else:
                    logging.info(
                        f"Insufficient Price History - CIK={CIK} - PERMNO={permno_set} - "
                        f"date_range='{date_start}, {date_finish}' {len(daily_price.dropna())}/{HOLDING_PERIOD}'")
                # skip this signal event, if no or insufficient price data
                continue
            # log CIK, event date with fewer historical vol data
            if len(daily_vol.dropna()) < TRACE_BACK_PERIOD:
                if len(daily_vol) < 10:
                    logging.info(f"vol History less than 10 - CIK={CIK} - PERMNO={permno_set} "
                                 f"- date_range='{date_start}, {date_finish}' vol nums: {len(daily_vol.dropna())} ")
                    # skip this signal event, if there's very few vol data
                    continue

            # adjust to price levels (for stock splits etc.)
            daily_price['prc'] = daily_price['prc'].abs() / daily_price['cfacpr']
            # compute trading volumne on event day as percentage of average for past X days  
            daily_vol['vol'] = daily_vol['vol'].abs() * daily_vol['cfacshr']
            vol_pct = daily_vol['vol'].iloc[-3:].mean() / daily_vol['vol'].iloc[:-3].mean()

            # create column labels
            date_finish = daily_price['date'].to_list()[-1].strftime("%Y-%m-%d")
            master_daily_price_dict[f"{CIK}_{date_prior}_{date_start}"] = daily_price['prc'].to_list()
            master_vol_pct_dict[f"{CIK}_{date_prior}_{date_start}"] = [vol_pct]

    # first price in res_price_df is end of day before signal event date
    res_price_df = pd.DataFrame.from_dict(master_daily_price_dict, orient='columns')
    res_vol_df = pd.DataFrame.from_dict(master_vol_pct_dict, orient='columns').T
    if res_vol_df.empty:
        res_vol_df = None
    else:
        res_vol_df.columns = ['vol']
    print(
        f"{len(res_price_df.columns)}/{sum([len(BUY_SIGNAL[CIK]) for CIK in BUY_SIGNAL])} price history generated from wrds/crsp")
    # logging.info(f"END wrds/crsp")
    return res_price_df, res_vol_df, ERROR_event


def price_history_to_cumu(price_history):
    """
        helper method that returns cumulative return of a series of daily price
    """
    if price_history.empty:
        raise ValueError("Empty pandas DataFrame: price_history")
    return (price_history.pct_change().fillna(0) + 1).cumprod()


def get_return_stats(db, price_history, CIK_SIC_mapping, SIC_portfolio_mapping, industry_portfolio_type=49):
    """
        returns a DataFrame that computes return statistics of each event
        returned features:
            ER :=
    """
    if price_history.empty:
        raise ValueError("Empty pandas DataFrame: price_history")

    # calculation of return starts on the event day
    # price_history.iloc[0] is price data on a day prior to event
    cumu_price_history = price_history_to_cumu(price_history.iloc[1:])

    # TOTAL_PERIOD:=HOLDING_PERIOD+1, +1 is for day -1 relative to event day
    TOTAL_PERIOD = len(price_history)
    EXCESS_RETURN_CALCULATION_PARAMS = [1, 10, 30, 60, 90, 120]  # days
    excess_returns = dict()
    excess_returns_industry = dict()
    IDT = dict()

    # m1, 0, 1 corresponds to date -1 , 0 , 1 relative event day
    datem1 = dict()
    date0 = dict()
    date1 = dict()
    R = dict()
    MKT = dict()

    for col in price_history:
        """
        col:= "CIK number"_"date prior"_"event date"
        """

        event_daily_change = price_history[col]
        prior_date, event_date = col.split('_')[1:]

        # obtain fama french data
        fama_french = db.raw_sql(
            f"SELECT * FROM ff.factors_daily WHERE cast(date as DATE) >= '{prior_date}' LIMIT {TOTAL_PERIOD}")

        # # compute alpha, beta
        # X = sm.add_constant(fama_french[['mktrf', 'smb', 'hml']])
        # y = event_daily_change.values - fama_french['rf']
        # res = sm.OLS(y, X).fit()
        # alpha, beta, *args = res.params
        # alpha_p, beta_p, *args = res.pvalues

        R_val = (event_daily_change.pct_change().dropna().iloc[:2] + 1).cumprod().tolist()[-1]
        MKT_val = (fama_french['mktrf'].iloc[1:3] + 1).cumprod().tolist()[-1]
        R[col] = R_val
        MKT[col] = MKT_val
        # ER:= Excess Return = cumulative return from day -1 to 1 minus cumulative return of market
        # ER[col] = R_val - MKT_val

        # excess market returns
        event_cumulative_change = (event_daily_change.pct_change().dropna() + 1).cumprod().tolist()
        market_cumulative_change = (fama_french['mktrf'].iloc[1:] + 1).cumprod().tolist()
        for num_days in EXCESS_RETURN_CALCULATION_PARAMS:
            if num_days not in excess_returns:
                excess_returns[num_days] = dict()
            excess_returns[num_days][col] = (event_cumulative_change[num_days] - market_cumulative_change[
                num_days])

        # excess industry portfolio returns
        CIK = int(col.split("_")[0])
        SIC = str(CIK_SIC_mapping[CIK])
        try:
            # there exists spurious SIC code from wrds mapping table, e.g. 6797
            industry_portfolio_name = SIC_portfolio_mapping[SIC]
        except KeyError as e:
            # catch the error and set industry benchmark data as nan
            logging.exception(e)
            for num_days in EXCESS_RETURN_CALCULATION_PARAMS:
                if num_days not in excess_returns_industry:
                    excess_returns_industry[num_days] = dict()
                excess_returns_industry[num_days][col] = np.nan
            IDT[col] = np.nan
            continue
        # read benchmark industry portfolio data
        industry_pflo = pd.read_csv(
            f"data/industry_classification_and_portfolio/{industry_portfolio_type}_Industry_Portfolios_Daily.CSV",
            header=5,
            index_col=0).iloc[:-1]
        industry_pflo.index = pd.to_datetime(industry_pflo.index, format="%Y%m%d")
        industry_pflo.columns = [col_name.rstrip() for col_name in industry_pflo.columns]
        industry_pflo.astype(np.float64)
        industry_cumulative_change = (
                industry_pflo.loc[event_date:][industry_portfolio_name] * 0.01 + 1).cumprod().tolist()
        for num_days in EXCESS_RETURN_CALCULATION_PARAMS:
            if num_days not in excess_returns_industry:
                excess_returns_industry[num_days] = dict()
            excess_returns_industry[num_days][col] = (
                        event_cumulative_change[num_days] - industry_cumulative_change[num_days])
        IDT[col] = industry_portfolio_name

        # save computed variables
        datem1[col] = prior_date
        date0[col] = event_date
        date1[col] = fama_french['date'].to_list()[2]

    master_dict = {
        'day -1': datem1,
        'day 0': date0,
        # 'day 1': date1,
        'R': R,
        'MKT': MKT,
    }

    for num_days in EXCESS_RETURN_CALCULATION_PARAMS:
        master_dict[f"{num_days}d - MKT"] = excess_returns[num_days]
    master_dict['IDT'] = IDT
    for num_days in EXCESS_RETURN_CALCULATION_PARAMS:
        master_dict[f"{num_days}d - IDT"] = excess_returns_industry[num_days]

    res = pd.DataFrame.from_dict(master_dict).reset_index()
    res['index'] = res['index'].apply(lambda row: row.split("_")[0])
    new_columns = res.columns.values
    new_columns[0] = "CIK"
    res.columns = new_columns
    res.set_index(['CIK', 'day -1', 'day 0'], inplace=True)  # multi-index

    return res
