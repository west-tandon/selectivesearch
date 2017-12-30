import argparse
import pandas as pd
import fastparquet as fp

parser = argparse.ArgumentParser(description='Produce a parquet file with relevant documents for a given index.', prog='produce-relevant')
parser.add_argument('relevant_titles', help='A file that maps query ID to relevant documents titles.')
parser.add_argument('idmapping', help='A file that maps titles to the document IDs in the index.')
parser.add_argument('output', help='The output file with mapping from query ID to document ID.')
args = parser.parse_args()


relevant_titles = fp.ParquetFile(args.relevant_titles).to_pandas()
idmapping = fp.ParquetFile(args.idmapping).to_pandas()

relevant_ids = pd.merge(relevant_titles, idmapping, on='title', sort=True)
relevant_ids.drop('title', inplace=True, axis=1)
fp.write(args.output, relevant_ids, compression='SNAPPY')
