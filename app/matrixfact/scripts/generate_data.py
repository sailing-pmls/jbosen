#!/usr/bin/env python

import argparse
import random
import sys

desc = ('''
Creates a sparse block-diagonal matrix, where each diagonal block has width and
height <block-width>; <num-diag-blocks> is the total number of diagonal blocks.
The upper left block is set to 1, and the i-th diagonal block is set to i. All
blocks are output to <output-file>. In addition, <off-diag-density>
approximately controls the fraction of off-diagonal entries (all of which are 0)
that are also written to <output-file>.

When <output-file> is input to Petuum MF with rank = <num-diag-blocks>, the
resulting L,R factors should be clearly split into <num-diag-blocks> groups of
<block-width> rows each. Thus, one may use this script to test Petuum MF on
arbitrarily-sized data. Note that the MF initial step size will have be tuned to
each new matrix, to prevent algorithm divergence.

Example:

python %s 10 100 0.1 test-matrix

This creates a 1000-by-1000 matrix with 100 diagonal blocks of width 10, and
outputs it to test-matrix; 10%% of off-diagonal entries are also written out.'''
% sys.argv[0])

parser = argparse.ArgumentParser(
  formatter_class=argparse.RawDescriptionHelpFormatter,
  description=desc,
)
parser.add_argument('block_width', type=int, help='Width and height of each diagonal block.')
parser.add_argument('num_diag_blocks', type=int, help='Total number of diagonal blocks.')
parser.add_argument('off_diag_density', type=float, help='Approximate fraction of off-diagonal entries (all of which are 0) that are also output.')
parser.add_argument('output_file', type=str, help='Path to output file.')

args = parser.parse_args()

block_width = args.block_width
num_diag_blocks = args.num_diag_blocks
off_diag_density = args.off_diag_density
output_file = args.output_file

N = block_width * num_diag_blocks
with open(output_file,'w') as g:
  for b in range(num_diag_blocks):
    for subrow in range(block_width):
      row_idx = subrow + b*block_width
      row_entries = {}
      # Generate random off-diagonal zeros
      for a in range(int(N*off_diag_density)):
        row_entries[random.randint(0,N-1)] = 0
      # Generate block diagonal
      for j in range(b*block_width,(b+1)*block_width):
        row_entries[j] = b
      # Print entries
      for el in row_entries.iteritems():
        g.write('%d %d %.1f\n' % (row_idx,el[0],el[1]))
