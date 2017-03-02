import argparse
import sys
import pandas as pd
import numpy as np

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
data.columns = ['qid', 'did', 'score', 'cscore']

with open('{}.results.global'.format(args.output), 'w') as results:
    with open('{}.scores'.format(args.output), 'w') as scores:
        for qid, group in data.groupby('qid'):
            sorted = [(int(row[0]), row[1]) for row in group.sort_values(by='cscore', ascending=False)[['did', 'cscore']].values]
            # print(sorted)
            results.write(' '.join([str(m(did)) for did, s in sorted]))
            results.write('\n')
            scores.write(' '.join([str(s) for did, s in sorted]))
            scores.write('\n')
