from os import listdir
from os.path import isfile, join
import pandas as pd

# function to read in the market data from CSV files
def ReadFiles(path, contract):
    files = [f for f in listdir(path) if isfile(join(path, f))]
    data = dict()
    for f in files:
        key = f[:15]
        c = f [:2]
        if c != contract:
            continue

        data[key] = pd.read_csv(join(path, f))
        
    return data