import argparse
import sys
import math
import json

parser = argparse.ArgumentParser(description='Aggregate membership by percentiles', prog='shard-membership-agg')
parser.add_argument('shard_count', type=int)
parser.add_argument('membership', nargs='?', type=argparse.FileType('r'),  help='A file with list of shard membership for each document', default=sys.stdin)
parser.add_argument('--percentiles', '-p', nargs='*', default=[10, 50, 100])

args = parser.parse_args()

membership = [int(line[:-1]) for line in args.membership.readlines()]
data = {}

for p in args.percentiles:
    percentile_size = math.ceil(len(membership) / p)
    pm = [[0 for i in range(p)] for s in range(args.shard_count)]
    for idx, shard in enumerate(membership):
        pm[shard][math.floor(idx / percentile_size)] += 1
    data[p] = pm

print(json.dumps(data))
