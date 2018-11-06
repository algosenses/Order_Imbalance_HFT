## SELL SIGNAL (at t)
## * E[ FPC (t)] <= -0.2
##
## if signal hits, buy or sell maximum position
# ###################################################
import pandas as pd
import numpy as np
import math
from BuildLinearData import *
from Session import *

def LinearStrategy(key,
                   data,
                   coefs,
                   session = Session.Morning,
                   open_int = False,
                   config = None,
                   functions = None):

    lags = int(config['trading']['lags'])
    strategy = config['trading']['strategy']
    threshold = float(config['trading']['threshold'])
    TR_COST = float(config['trading']['tradecost']) # 2.5 * 1e-5,
    trade_at_mid = True if config['trading']['trade_at_mid'] == 'True' else False

    ## get all the market data (this would be a data-stream in a real-time system)
    value = BuildLinearData(data,
                            session = session,
                            open_int = open_int,
                            delay = 0,
                            lags = lags,
                            config = config,
                            functions = functions)

    main_data = value['data']
    n = len(main_data)

    mid_price = value['mid_price']
    spread = value['spread']
    time_secs = value['time_secs']
    ind_open = value['ind_open']
    ind_close = value['ind_close']

    own = False
    pos = 0
    strat = [ 0 ] * n
    realized_pnl = [ np.nan ] * n
    total_trade_pnl = []
    returns = []
    pnl = 0
    trade_costs = 0
    buy_price = 0
    sell_price = 0
    entry = 0
    trade_volume = 0
    trade_records = []
    sharpes = []


    # get the vector of bid/ ask prices (this will be scalar in data stream)
    ask = mid_price if trade_at_mid else main_data['AskPrice1']
    bid = mid_price if trade_at_mid else main_data['BidPrice1']

    # Set the x-values to be used in prediction depending on strategy
    # these would be scalar in a data stream
    VOI = value['VOI']
    OIR = value['OIR']
    MPB = value['MPB']

    x = [ 1 ] * n
    if strategy == 'A':
        x = pd.concat([pd.Series(x).rename('const'), VOI], axis = 1)
    elif strategy == 'B':
        x = pd.concat([pd.Series(x).rename('const'), VOI.div(spread, axis = 0), OIR.div(spread, axis = 0), MPB.iloc[:,0].div(spread, axis = 0).rename('MPB')], axis = 1)
    else:
        print('Missing Linear Strategy: %s' % strategy)
        exit()

    # this is where we assume we get a data stream instead of looping through the dataset
    # multiply the coefficients with the factors and check if it's above / below threshold
    # and trade if the signal is good

    # in an actual trading system, the decision would be calculated by a strategy engine
    # having the real-time data fed into the engine via a data stream
    # but in this simulation, we just assume we have the full dataset and the
    # strategy engine is the coefficient multiplication on the next line
    efpc_vec = x * pd.DataFrame().append([coefs] * n).reset_index(drop = True)
    efpc_vec = efpc_vec.sum(axis = 1)

    trade_ind = range(0, n)
    for k in trade_ind:
        efpc = efpc_vec[k]
        ## check if we are within trading hours
        if k >= ind_open and k < ind_close and own == False and efpc >= threshold:
            ## BUY to OPEN
            strat[k] = 1
            own = True
            pos = 1
            buy_price = ask[k]
            entry = k
            tc = buy_price * TR_COST
            trade_costs = trade_costs + tc
            trade_volume = trade_volume + 1
            trade_records.append((key, main_data['UpdateTime'][k] + '.' + str(main_data['UpdateMillisec'][k]), 'BUY', 1, buy_price, tc, 0))
        elif k >= ind_open and k < ind_close and own == False and efpc <= -threshold:
            ## SELL to OPEN
            strat[k] = -1
            own = True
            pos = -1
            sell_price = bid[k]
            entry = k
            tc = sell_price * TR_COST
            trade_costs = trade_costs + tc
            trade_volume = trade_volume + 1
            trade_records.append((key, main_data['UpdateTime'][k] + '.' + str(main_data['UpdateMillisec'][k]), 'SHORT', 1, sell_price, tc, 0))
        elif own == True and pos == 1 and efpc <= -threshold:
            ## SELL to CLOSE
            strat[k] = -1
            own = False
            pos = 0
            sell_price = bid[k]
            tc = tc + sell_price * TR_COST
            trade_costs = trade_costs + tc
            trade_pnl = sell_price - buy_price - tc
            pnl = pnl + trade_pnl
            trade_volume = trade_volume + 1
            total_trade_pnl.append(trade_pnl)
            trade_records.append((key, main_data['UpdateTime'][k] + '.' + str(main_data['UpdateMillisec'][k]), 'SELL', 1, sell_price, tc, trade_pnl))

            if k >= ind_open and k < ind_close:
                  ## SELL to OPEN
                  strat [k] = -2
                  own = True
                  pos = -1
                  sell_price = bid[k]
                  entry = k
                  tc = sell_price * TR_COST
                  trade_costs = trade_costs + tc
                  trade_volume = trade_volume + 1
                  trade_records.append((key, main_data['UpdateTime'][k] + '.' + str(main_data['UpdateMillisec'][k]), 'SHORT', 1, sell_price, tc, 0))
        elif own == True and pos == -1 and efpc >= threshold:
            ## BUY to CLOSE
            strat [k] = 1
            own = False
            pos = 0
            buy_price = ask[k]
            tc = tc + buy_price * TR_COST
            trade_costs = trade_costs + tc
            trade_pnl = sell_price - buy_price - tc
            pnl = pnl + trade_pnl
            trade_volume = trade_volume + 1
            total_trade_pnl.append(trade_pnl)
            trade_records.append((key, main_data['UpdateTime'][k] + '.' + str(main_data['UpdateMillisec'][k]), 'COVER', 1, buy_price, tc, trade_pnl))

            if k >= ind_open and k < ind_close:
                  ## BUY to OPEN
                  strat [k] = 2
                  own = True
                  pos = 1
                  buy_price = ask[k]
                  entry = k
                  tc = buy_price * TR_COST
                  trade_costs = trade_costs + tc
                  trade_volume = trade_volume + 1
                  trade_records.append((key, main_data['UpdateTime'][k] + '.' + str(main_data['UpdateMillisec'][k]), 'BUY', 1, buy_price, tc, 0))

        realized_pnl[k] = pnl

    # check if we have a left-over position at end-of-day and close it
    if sum(strat) == 1:
      if strat[n-1] == 1:
        strat[n-1] = 0
        trade_volume = trade_volume - 1
      else:
        strat[n-1] = -1
        sell_price = bid[n-1]
        tc = tc + sell_price * TR_COST
        trade_costs = trade_costs + tc
        trade_pnl = sell_price - buy_price - tc
        pnl = pnl + trade_pnl
        realized_pnl[n-1] = pnl
        total_trade_pnl.append(trade_pnl)
        trade_volume = trade_volume + 1
        trade_records.append((key, main_data['UpdateTime'][n-1] + '.' + str(main_data['UpdateMillisec'][n-1]), 'SELL', 1, sell_price, tc, trade_pnl))
    elif sum(strat) == -1:
      if strat[n-1] == -1:
        strat[n-1] = 0
        trade_volume = trade_volume - 1
      else:
        strat[n-1] = 1
        buy_price = ask[n-1]
        tc = tc + buy_price * TR_COST
        trade_costs = trade_costs + tc
        trade_pnl = (sell_price - buy_price) - tc
        pnl = pnl + trade_pnl
        realized_pnl[n-1] = pnl
        total_trade_pnl.append(trade_pnl)
        trade_volume = trade_volume + 1
        trade_records.append((key, main_data['UpdateTime'][n-1] + '.' + str(main_data['UpdateMillisec'][n-1]), 'COVER', 1, buy_price, tc, trade_pnl))

    # return stats
    if math.isnan(realized_pnl[0]):
        realized_pnl[0] = 0

    realized_pnl = pd.Series(realized_pnl).fillna(method='ffill').tolist()

    value = {}
    value['time'] = time_secs
    value['pnl'] = realized_pnl
    value['strategy'] = strat
    value['trade_records'] = trade_records
    value['trade_volume'] = trade_volume
    value['trade_pnl'] = total_trade_pnl
    value['trade_costs'] = trade_costs
    return (value)