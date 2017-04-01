import argparse
import pandas as pd
from sys import stdout



parser = argparse.ArgumentParser(description='Aggregate verbose results', prog='aggregate-verbose')
parser.add_argument('input', help='Input file with verbose results')

args = parser.parse_args()

data = pd.read_csv(args.input)
columns_to_remove = ['qid', 'last_shard', 'last_bucket']
data[[column for column in data.columns if column not in columns_to_remove]].groupby('step').mean().round(4)\
    .to_csv('{}.aggregated'.format(args.input))