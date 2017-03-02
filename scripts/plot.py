import argparse
import sys
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np


parser = argparse.ArgumentParser(description='Plot CSV', prog='plot')
parser.add_argument('input', nargs='*', type=argparse.FileType('r'), default=[sys.stdin])
parser.add_argument('--x-column', '-x', nargs='?', default=None)
parser.add_argument('--y-column', '-y', nargs='?', default=None)
parser.add_argument('--legend', '-l', nargs='*', default=None)
args = parser.parse_args()

if args.legend is not None:
    assert len(args.legend) == len(args.input), "the number of legend labels must match the number of input files"

handles = []
for f in args.input:
    data = pd.read_csv(f)
    x_col = args.x_column if args.x_column is not None else data.columns[0]
    y_col = args.y_column if args.y_column is not None else data.columns[1]
    handles.append(plt.plot(np.array(data[x_col]), np.array(data[y_col]))[0])
print(handles)
print(args.legend)
plt.legend(handles, args.legend)
plt.show()

