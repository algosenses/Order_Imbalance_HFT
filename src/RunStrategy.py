import pandas as pd
from collections import namedtuple
from ReadFiles import *
from BuildLinearModel import *
from LinearStrategy import *
import Config
from Session import *

def run(config_file):
    config = Config.read(config_file)

    path     = config['global']['datapath']
    contract = config['global']['contract']

    # one day' data corresponds to a key
    data = ReadFiles(path, contract)

    #### AVERAGED LAG LINEAR STRATEGY ####

    ## set trading and model parameters
    threshold = float(config['trading']['threshold'])
    period    = int(config['trading']['period'])
    lags      = int(config['trading']['lags'])
    strategy  = config['trading']['strategy']
    night     = 'night' in config['global']['sessions']
    fulldaycoefs = True if config['global']['fulldaycoefs'] != 'False' else False

    keys = []
    coefs = []
    params = []

    ## build the linear models and store their coefficients
    for k, v in sorted(data.items()):
        # full-day coefficients
        if fulldaycoefs:
            value = BuildLinearModel(k,
                                     v,
                                     session = Session.FullDay,
                                     delay = period,
                                     lags = lags,
                                     config = config)
            model = value['model']
            coefs.append(model.params)
            params.append([k, v, Session.FullDay, model.params])
        else:
            value = BuildLinearModel(k,
                                     v,
                                     session = Session.Morning,
                                     delay = period,
                                     lags = lags,
                                     config = config)
            model = value['model']
            coefs.append(model.params)
            params.append([k, v, Session.Morning, model.params])

            value = BuildLinearModel(k,
                                     v,
                                     session = Session.Afternoon,
                                     delay = period,
                                     lags = lags,
                                     config = config)
            model = value['model']
            coefs.append(model.params)
            params.append([k, v, Session.Afternoon, model.params])

        keys.append(k)

        print('Instrument: %s' % k)
        for key, val in sorted(model.params.items()):
            print("%s: %.10f" % (key, val))

    return

    StratParam = namedtuple('StratParam', ['key', 'data', 'session', 'coef'])

    strat_params = []
    for idx, val in enumerate(params):
        if idx > 0:
            strat_params.append(StratParam(val[0], val[1], val[2], coefs[idx-1]))
        else:
            strat_params.append(StratParam(val[0], val[1], val[2], val[3]))

    coefs = pd.DataFrame(coefs)

    ## set the lagged coefficient weights
    coef_weights = [1]
    trade_volume = []
    trade_costs = []
    trade_records = []

    pnl_name = 'pnl-%.1f-%s-%d-F-lag %d' % (threshold, strategy, period, lags)

    pnl_matrix = pd.DataFrame(index = range(0, len(keys)), columns = ['morning', 'afternoon'])
    pnl_matrix.iloc[0] = 0
    trade_pnl = []

    ## apply the trading strategy to each trading day using historical linear model coefficients
    for idx, val in enumerate(strat_params):
        key = val.key
        data = val.data
        row = 0
        col = 0

        if idx > 0:
    #        coef = 0
    #        w = coef_weights[:min(len(coef_weights), i)]
    #        w = w / sum(w)
    #        for j in range(0, len(w)):
    #            coef = coef + coefs[i - j, ] * w[j]
            coef = val.coef

            if val.session == Session.FullDay:
                row = idx
                # morning trading using the weighted coefficients from T-1, T-2 ,...
                strat = LinearStrategy(
                    key,
                    data,
                    coef,
                    session = Session.Morning,
                    config = config)
                pnl_matrix.iloc[row, 0] = strat['pnl'][-1]
                trade_pnl.append(strat['trade_pnl'])
                tv = strat['trade_volume']
                tc = strat['trade_costs']
                trade_records.extend(strat['trade_records'])

                # afternoon trading using the weighted coefficients from T-1, T-2 ,...
                strat = LinearStrategy(
                    key,
                    data,
                    coef,
                    session = Session.Afternoon,
                    config = config)
                pnl_matrix.iloc[row, 1] = strat['pnl'][-1]
                trade_pnl.append(strat['trade_pnl'])
                tv = tv + strat['trade_volume']
                trade_volume.append(tv)
                tc = tc + strat['trade_costs']
                trade_costs.append(tc)
                trade_records.extend(strat['trade_records'])

            else:
                strat = LinearStrategy(
                    key,
                    data,
                    coef,
                    session = val.session,
                    config = config)
                row = int(idx / 2)
                col = idx % 2
                pnl_matrix.iloc[row, col] = strat['pnl'][-1]
                trade_pnl.append(strat['trade_pnl'])
                tv = strat['trade_volume']
                tc = strat['trade_costs']
                trade_records.extend(strat['trade_records'])

    for idx, key in enumerate(keys):
        print('%s %s %d %.1f P&L = %f %f Total = %f' % (key, strategy, period, threshold, pnl_matrix.iloc[idx, 0], pnl_matrix.iloc[idx, 1], pnl_matrix.iloc[0:idx+1, :].values.sum()))

    day_pnl_mat = pnl_matrix.sum(axis = 1)
    sharpe_ratio = day_pnl_mat.values.mean() * math.sqrt(len(day_pnl_mat)) / day_pnl_mat.values.std(ddof=1)

    pnl_matrix.insert(0, 'instrument', pd.Series(sorted(data.keys())))
    pnl_matrix.to_csv(pnl_name + '.csv', sep = ',', index=False)

    trade_records = pd.DataFrame(trade_records, columns=['Date', 'Time', 'Action', 'Qty', 'Price', 'TradeCost', 'PnL'])
    trade_records.to_csv('trade_records.csv', sep = ',', index=False)

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Backtest strategy with user configuration.')
    parser.add_argument('-c', '--config', help='Configuration.', default='Config.ini')
    args = parser.parse_args()

    config_file = args.config

    run(config_file)
