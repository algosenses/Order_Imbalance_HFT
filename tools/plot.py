import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import statsmodels.api as sm
import seaborn as sns
import download

def plot_voi(csvfile, delay = 20, lags = 5):
    delay = delay
    lags = lags
    morning = True
    data = pd.read_csv(csvfile)

    AM = dict()
    AM['start'] = 34200
    AM['open'] = 34260
    AM['close'] = 40800
    AM['end'] = 41280

    PM = dict()
    PM['start'] = 46800
    PM['open'] = 46860
    PM['close'] = 53100
    PM['end'] = 53880

    start_time = AM['start'] if morning else PM['start']    # - data start
    open_time  = AM['open']  if morning else PM['open']     # - trade open
    close_time = AM['close'] if morning else PM['close']    # - trade close
    end_time   = AM['end']   if morning else PM['end']      # - data end

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

    def _get_bid_cv(bv, sbv, dbp):
        if dbp == 0:
            return bv - sbv
        elif dbp < 0:
            return 0
        else:
            return bv

    df['bid_CV'] = df.apply(lambda row : _get_bid_cv(row['bv'], row['sbv'], row['dbp']), axis=1)
    bid_CV = df['bid_CV']

    df = pd.concat([main_data['AskVolume1'], main_data['AskVolume1'].shift(1).fillna(0), dAsk_price], axis=1)
    df.columns = ['av', 'sav', 'dap']

    def _get_ask_cv(av, sav, dap):
        if dap == 0:
            return av - sav
        elif dap < 0:
            return av
        else:
            return 0

    df['ask_CV'] = df.apply(lambda row : _get_ask_cv(row['av'], row['sav'], row['dap']), axis=1)
    ask_CV = df['ask_CV']

    VOI_array = bid_CV - ask_CV

    k = delay
    p = lags

    rolling_mean = mid_price.rolling(center=False, window=k).mean().iloc[k-1:].reset_index(drop=True)
    # rolling_mean = mid_price.shift(-k).iloc[k:].reset_index(drop=True)
    fpc = rolling_mean - mid_price[:(n-k+1)]

    y = mid_price.shift(-(delay-1)) - mid_price
    fpc.shift(-(k-1))[:(n-k+1)]
    x = VOI_array.rolling(center=False, window=lags).sum().shift(-(lags-1))[:(n-k+1)]

    model = sm.OLS(fpc, sm.add_constant(x)).fit()
    print(model.summary())

    fig, ax = plt.subplots()

    ax.scatter(x, fpc, marker='.')
    fig.suptitle('VOI vs Price change')

    x_pred = np.linspace(x.min(), x.max(), 50)
    x_pred2 = sm.add_constant(x_pred)
    y_pred = model.predict(x_pred2)
    ax.plot(x_pred, y_pred, '-', color='darkorchid', linewidth=2)

    plt.show()

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='download ticks from GoldMiner.')
    parser.add_argument('-e', '--exchange', help='exchange.', required=True)
    parser.add_argument('-i', '--instrument', help='instrument.', required=True)
    parser.add_argument('-d', '--date', help='quote date.', required=True)
    args = parser.parse_args()

    exchange = args.exchange
    instrument = args.instrument
    date = args.date

    csvfile = download.download(exchange, instrument, date)

    if csvfile is not None:
        plot_voi(csvfile, 20, 5)