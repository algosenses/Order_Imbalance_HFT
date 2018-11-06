import dateutil.parser
import os.path
import datetime, time
import csv
import math
from gmsdk import md

def download(exchange, instrument, date):
    dt = dateutil.parser.parse(date)

    fname = '%s-%4d%02d%02d.csv' % (instrument, dt.year, dt.month, dt.day)
    if os.path.isfile(fname):
        print('%s already existed!' % fname)
        return fname

    print('download ticks \'%s\'...' % fname)

    start = '%4d-%02d-%02d 09:00:00' % (dt.year, dt.month, dt.day)
    end   = '%4d-%02d-%02d 15:00:00' % (dt.year, dt.month, dt.day)

    md.init('xxx@xxx.com', 'xxx')

    data = md.get_ticks('%s.%s' % (exchange, instrument), start, end)

    if len(data) == 0:
        print('download failed!')
        return None
    else:
        print('download %d ticks' % len(data))

    def secondofday(time):
        t = time.split(':')
        return int(t[0]) * 3600 + int(t[1]) * 60 + int(t[2])

    rnd = lambda x : int(x) if math.modf(x)[0] == 0 else round(x, 1)

    ticks = []
    for x in data:
        utctime = datetime.datetime.fromtimestamp(x.utc_time)
        time = utctime.strftime('%H:%M:%S')
        millis = int(int(utctime.strftime('%f')) / 1000)
        seconds = secondofday(time)
        ticks.append([x.sec_id, time, millis, int(x.cum_volume), rnd(x.cum_amount), x.cum_position, rnd(x.bids[0][0]), x.bids[0][1], rnd(x.asks[0][0]), x.asks[0][1], seconds])

    with open(fname, 'w') as outfile:
        writer = csv.writer(outfile, delimiter=',')
        writer.writerow(["InstrumentID", "UpdateTime", "UpdateMillisec", "Volume", "Turnover", "OpenInterest", "BidPrice1", "BidVolume1", "AskPrice1", "AskVolume1", "SecondOfDay"])
        for x in ticks:
            writer.writerow(x)

    return fname

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

    download(exchange, instrument, date)