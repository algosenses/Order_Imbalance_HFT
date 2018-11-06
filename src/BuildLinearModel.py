import matplotlib.pyplot as plt
import statsmodels.api as sm
import pandas as pd
from BuildLinearData import *
from Session import *

def BuildLinearModel(key,
                    data,
                    session = Session.FullDay,
                    morning = True,
                    open_int = False,
                    delay = 20,
                    lags = 5,
                    config = None,
                    functions = None):
    
    strategy = config['trading']['strategy']

    # check if we need a full-day linear model or for a single trading session
    if session == Session.FullDay:
        morning_data = BuildLinearData(data, 
                            session = Session.Morning,
                            open_int = open_int,
                            delay = delay,
                            lags = lags,
                            config = config,
                            functions = functions)
        evening_data = BuildLinearData(data,
                            session = Session.Afternoon,
                            open_int = open_int,
                            delay = delay,
                            lags = lags,
                            config = config,
                            functions = functions)
        dMid_Response = morning_data['dMid_Response'].append(evening_data['dMid_Response'], ignore_index=True)
        VOI = pd.concat([morning_data['VOI'], evening_data['VOI']], ignore_index=True)
        OIR = pd.concat([morning_data['OIR'], evening_data['OIR']], ignore_index=True)
        time_secs = morning_data['time_secs'].append(evening_data['time_secs'], ignore_index=True)
        mid_price = morning_data['mid_price'].append(evening_data['mid_price'], ignore_index=True)
        spread = morning_data['spread'].append(evening_data['spread'], ignore_index=True)
        AvgTrade_price = morning_data['AvgTrade_price'].append(evening_data['AvgTrade_price'])
        MPB = pd.concat([morning_data['MPB'], evening_data['MPB']], ignore_index=True)
        trading_data = pd.concat([morning_data['data'], evening_data['data']], ignore_index=True)
    else:
        trading_data = BuildLinearData(
                          data,
                          session = session,
                          open_int = open_int,
                          delay = delay,
                          lags = lags,
                          config = config,
                          functions = functions)
        dMid_Response = trading_data['dMid_Response']
        VOI = trading_data['VOI']
        OIR = trading_data['OIR']
        time_secs = trading_data['time_secs']
        mid_price = trading_data['mid_price']
        spread = trading_data['spread']
        AvgTrade_price = trading_data['AvgTrade_price']
        MPB = trading_data['MPB']
        trading_data = trading_data['data']

    ## build the features matrix (x-variable ) based on strategy
    ## transform the variables if necessary
    

    ## build the explanatory variables
    Y = dMid_Response
    x = dict()
    if strategy == 'A':
        X = sm.add_constant(VOI)
        x['A'] = X
        model = sm.OLS(Y, X).fit()
    elif strategy == 'B':
        X = pd.concat([VOI.div(spread, axis = 0), OIR.div(spread, axis = 0), MPB.iloc[:,0].div(spread, axis = 0).rename('MPB')], axis = 1)
        x['B'] = X
        model = sm.OLS(Y, sm.add_constant(X)).fit()

    value = dict()
    ## return values
    value['dMid_Response'] = dMid_Response ## y-value
    value['VOI'] = VOI
    value['OIR'] = OIR
    value['spread'] = spread
    value['y'] = dMid_Response
    value['x'] = x
    value['model'] = model
    value['data'] = trading_data
    value['AvgTrade_price'] = AvgTrade_price
    value['mid_price'] = mid_price
    value['MPB'] = MPB
    value['time_secs'] = time_secs

    return value