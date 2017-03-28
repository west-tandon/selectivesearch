import argparse
import re

parser = argparse.ArgumentParser(description='For documents in a global ordering, output their shard membership in the cluster', prog='shard-membership')
parser.add_argument('output_prefix', help='Output files prefix')
parser.add_argument('all_titles', help='A file containing all documents, sorted in global ordering.')
parser.add_argument('shard_titles', nargs='+', help='Files containing shards documents sorted in global ordering.')

args = parser.parse_args()

titles = [line[:-1] for line in open(args.all_titles).readlines()]
membership = [-1 for title in titles]

for shard in args.shard_titles:
    shard_id = int(re.search("-(\d+)", shard).group(1))
    with open(shard) as shardf:
        with open("{}#{}.membership".format(args.output_prefix, shard_id), 'w') as shardout:
            i = 0
            for shard_title in shardf.readlines():
                try:
                    i = titles.index(shard_title[:-1], i)
                except ValueError as e:
                    print("{} is not in all_titles; shard: {}, i: {}".format(shard_title[:-1], shard_id, i))
                    raise e
                membership[i] = shard_id
                shardout.write("{}\n".format(i))

with open(args.output_prefix + ".docrank", 'w') as f:
    for m in membership:
        f.write("{}\n".format(m))
