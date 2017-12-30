import argparse
import sys
import pandas as pd
import numpy as np
from fastparquet import write

parser = argparse.ArgumentParser(description='Convert a complex-ranking-function result file to a format supported by selectivesearch', prog='complex')
parser.add_argument('input', nargs='?', type=argparse.FileType('r'), default=sys.stdin)
parser.add_argument('output')
parser.add_argument('--map', '-m', type=argparse.FileType('r'))
parser.add_argument('--count', '-c', type=int)
args = parser.parse_args()

assert (args.map is None and args.count is None) or (args.map is not None and args.count is not None),\
    'you must define both map and count or neither of them'

map = None
if args.map is not None:
    map = [-1 for i in range(args.count)]
    for line in args.map.readlines():
        mm = line.split()
        map[int(mm[0])] = int(mm[1])

def m(id):
    if map is None:
        return id
    else:
        return map[id]

data = pd.read_csv(args.input, sep=' ')
data.columns = ['query', 'gdocid', 'score', 'cscore']
data['gdocid'] = data['gdocid'].map(lambda docid: map[docid])
data['ldocid'] = data['gdocid']
data = data.sort_values(by=['query', 'cscore'], ascending=[True, False])
data['rank'] = data.groupby('query').cumcount().astype(np.int32)
data['query'] = data['query'].rank(method='dense').subtract(1).astype(np.int32)

write('{}.complexresults'.format(args.output), data, compression='SNAPPY', write_index=False)
