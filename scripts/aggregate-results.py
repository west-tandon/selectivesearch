import argparse
from sys import stdout

parser = argparse.ArgumentParser(description='Aggregate OSS-format results', prog='aggregate-results')
parser.add_argument('basename', help='Basename of the results')
parser.add_argument('--trec-measures', '-t', nargs='*', default=['P_10', 'P_30'])
parser.add_argument('--overlap-measures', '-o', nargs='*', default=['10', '30'])
parser.add_argument('--budgets', '-b', nargs='*', default=None, type=int)

args = parser.parse_args()

def print_header():
    print('Bgt,{0},{1},Time Cost,#Shards'.format(
        ','.join([m.replace('_', '@') for m in args.trec_measures]),
        ','.join(['O@{}'.format(o) for o in args.overlap_measures])
    ))

def resolve_budgets():
    raise NotImplementedError()

if args.budgets is None:
    budgets = resolve_budgets()
else:
    budgets = args.budgets

print_header()
for budget in sorted(budgets, reverse=True):
    stdout.write('{},'.format(budget))
    with open('{0}$[{1}].trec.eval'.format(args.basename, budget)) as trec:
        line_tuples = [line[:-1].split() for line in trec.readlines()]
        measures = dict([(tup[0], tup[2]) for tup in line_tuples if tup[1] == 'all'])
        stdout.write(','.join([measures[m] for m in args.trec_measures]))
    stdout.write(',')
    for overlap in args.overlap_measures:
        with open('{0}$[{1}]@{2}.overlap'.format(args.basename, budget, overlap)) as ov:
            val = round(float(ov.read().strip()), 4)
            stdout.write('{},'.format(val))
    with open('{0}$[{1}].selection.time.avg'.format(args.basename, budget)) as time:
        val = round(float(time.read().strip()), 2)
        stdout.write('{},'.format(val))
    with open('{0}$[{1}].selection.shard-count.avg'.format(args.basename, budget)) as time:
        val = round(float(time.read().strip()), 2)
        stdout.write('{}'.format(val))
    stdout.write('\n')