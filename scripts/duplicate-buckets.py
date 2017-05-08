import argparse
import numpy as np
import pandas as pd
from fastparquet import ParquetFile, write

parser = argparse.ArgumentParser(description='Duplicate bucket 0 n times, producing n buckets', prog='duplicate-buckets')
parser.add_argument('input')
parser.add_argument('output')
parser.add_argument('--num-buckets', '-n', type=int)
args = parser.parse_args()

input_df = ParquetFile(args.input).to_pandas()

output_dfs = [input_df.copy(deep=True) for bucket in range(args.num_buckets)]
for bucket, df in enumerate(output_dfs):
    df['bucket'] = bucket
    df['bucket'] = df['bucket'].astype(np.int32)

write(args.output, pd.concat(output_dfs), compression='SNAPPY', write_index=False)
