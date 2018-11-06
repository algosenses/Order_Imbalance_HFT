import pandas as pd
import numpy as np
from Session import *

def BuildLinearData(data, 
                    session = Session.Morning, 
                    open_int = False, 
                    delay = 20,  # next 10 seconds (20 time-steps) average mid-price change
                    lags = 5,
                    config = None,
                    functions = None):
    
    # declare constants
    day = dict()
    day_start = 34200

    multiplier = float(config['trading']['multiplier'])

    def secondofday(time):
        t = time.split(':')
        return int(t[0]) * 3600 + int(t[1]) * 60 + int(t[2])

    morning = dict()
    morning['start'] = secondofday(config['morning']['start'])
    morning['open']  = secondofday(config['morning']['open'])
    morning['close'] = secondofday(config['morning']['close'])
    morning['end']   = secondofday(config['morning']['end'])

    afternoon = dict()
    afternoon['start'] = secondofday(config['afternoon']['start'])
    afternoon['open']  = secondofday(config['afternoon']['open'])
    afternoon['close'] = secondofday(config['afternoon']['close'])
    afternoon['end']   = secondofday(config['afternoon']['end'])

    night = dict()
    night['start'] = secondofday(config['night']['start'])
    night['open']  = secondofday(config['night']['open'])
    night['close'] = secondofday(config['night']['close'])
    night['end']   = secondofday(config['night']['end'])

    if session == Session.Morning:
        start_time = morning['start']       # - data start
        open_time  = morning['open']        # - trade open
        close_time = morning['close']       # - trade close
        end_time   = morning['end']         # - data end
    elif session == Session.Afternoon:
        start_time = afternoon['start']
        open_time  = afternoon['open']
        close_time = afternoon['close']
        end_time   = afternoon['end']
    elif session == Session.Night:
        start_time = night['start']
        open_time  = night['open']
        close_time = night['close']
        end_time   = night['end']


    ind = data[(data['SecondOfDay'] >= start_time) & (data['SecondOfDay'] < end_time)].index.values.astype(int)
    main_data = data.loc[ind].reset_index(drop=True)
    n = len(main_data.index)
    time_secs = main_data['SecondOfDay'] + main_data['UpdateMillisec'] / 1000
    ind_open  = time_secs[time_secs >= open_time].index.values.astype(int)[0]
    ind_close = time_secs[time_secs >= close_time].index.values.astype(int)[0]

    # calculate variables
    mid_price = (main_data['BidPrice1'] + main_data['AskPrice1']) / 2
    spread = main_data['AskPrice1'] - main_data['BidPrice1']

    OIR_array = (main_data['BidVolume1'] - main_data['AskVolume1']) / (main_data['BidVolume1'] + main_data['AskVolume1'])
    dBid_price = main_data['BidPrice1'].diff().fillna(0)
    dAsk_price = main_data['AskPrice1'].diff().fillna(0)

    ## build order imbalance signal according to Spec
    df = pd.concat([main_data['BidVolume1'], main_data['BidVolume1'].shift(1).fillna(0), dBid_price], axis=1)
    df.columns = ['bv', 'sbv', 'dbp']

    def get_bid_cv(bv, sbv, dbp):
        if dbp == 0:
            return bv - sbv
        elif dbp < 0:
            return 0
        else:
            return bv

    df['bid_CV'] = df.apply(lambda row : get_bid_cv(row['bv'], row['sbv'], row['dbp']), axis=1)
    bid_CV = df['bid_CV']

    df = pd.concat([main_data['AskVolume1'], main_data['AskVolume1'].shift(1).fillna(0), dAsk_price], axis=1)
    df.columns = ['av', 'sav', 'dap']

    def get_ask_cv(av, sav, dap):
        if dap == 0:
            return av - sav
        elif dap < 0:
            return av
        else:
            return 0

    df['ask_CV'] = df.apply(lambda row : get_ask_cv(row['av'], row['sav'], row['dap']), axis=1)
    ask_CV = df['ask_CV']

    VOI_array = bid_CV - ask_CV

    dVol = main_data['Volume'].diff()
    dTO = main_data['Turnover'].diff()
    AvgTrade_price = dTO / dVol / multiplier
    AvgTrade_price = AvgTrade_price.fillna(method='ffill').fillna(method='bfill')
    rolling_mean = mid_price.rolling(center=False, window=2).mean()
    rolling_mean.iloc[0] = mid_price.iloc[0]
    MPB_array = AvgTrade_price - rolling_mean

    k = delay
    p = lags
    new_ind = list(range(p, n - k))

    ## arithmetic average of future k midprices minus current midprice
    if k > 0:
        rolling_mean = mid_price.rolling(center=False, window=k).mean().iloc[k:].reset_index(drop=True)
#        rolling_mean = mid_price.shift(-k).iloc[k:].reset_index(drop=True)
        fpc = rolling_mean - mid_price[:(n-k)]
        dMid_Response = fpc.append(pd.Series([np.nan]*k))
    else:
        dMid_Response = pd.Series([0] * n)

    # build VOI , dMid , OIR - first p entries are useless
    VOI = pd.DataFrame()
    OIR = pd.DataFrame()
    MPB = pd.DataFrame()

    if p > 0:
        for j in range(0, p + 1):
            VOI = pd.concat([VOI, VOI_array.shift(j).rename('VOI.t%d' % j)], axis = 1)
            OIR = pd.concat([OIR, OIR_array.shift(j).rename('OIR.t%d' % j)], axis = 1)
            MPB = pd.concat([MPB, MPB_array.shift(j).rename('MPB.t%d' % j)], axis = 1)

    dMid_Response = dMid_Response.iloc[new_ind]
    VOI = VOI.iloc[new_ind]
    OIR = OIR.iloc[new_ind]
    MPB = MPB.iloc[new_ind]

    # trim the other supporting data
    mid_price = mid_price.iloc[new_ind]
    spread = spread.iloc[new_ind]
    AvgTrade_price = AvgTrade_price.iloc[new_ind]
    main_data = main_data.iloc[new_ind]
    time_secs = time_secs.iloc[new_ind]
    
    ind_open = ind_open - p
    ind_close = ind_close - p

    value = dict()
    value['data'] = main_data.reset_index(drop=True)
    value['dMid_Response'] = dMid_Response.reset_index(drop=True)
    value['VOI'] = VOI.reset_index(drop=True)
    value['OIR'] = OIR.reset_index(drop=True)
    value['MPB'] = MPB.reset_index(drop=True)

    value['time_secs'] = time_secs.reset_index(drop=True)
    value['ind'] = ind
    value['ind_open'] = ind_open
    value['ind_close'] = ind_close

    value['mid_price'] = mid_price.reset_index(drop=True)
    value['spread'] = spread.reset_index(drop=True)
    value['AvgTrade_price'] = AvgTrade_price.reset_index(drop=True)

    return value