import argparse
import numpy as np
import pandas as pd
from fastparquet import ParquetFile, write

parser = argparse.ArgumentParser(description='Duplicate bucket 0 n times, producing n buckets', prog='duplicate-buckets')
parser.add_argument('input_prefix')
parser.add_argument('output_prefix')
parser.add_argument('num-shards', type=int)
parser.add_argument('num-buckets', type=int)
args = parser.parse_args()


for shard in range(args.num_shards):
    input_df = ParquetFile("{}#{}.impacts".format(args.input, shard)).to_pandas()
    output_dfs = [input_df.copy(deep=True) for bucket in range(args.num_buckets)]
    for bucket, df in enumerate(output_dfs):
        df['bucket'] = bucket
        df['bucket'] = df['bucket'].astype(np.int32)
    write("{}#{}.impacts".format(args.output_prefix, shard), pd.concat(output_dfs), compression='SNAPPY', write_index=False)
