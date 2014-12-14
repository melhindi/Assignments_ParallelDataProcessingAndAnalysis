#!/usr/bin/env python
__author__ = 'developer'

from pyspark import SparkContext
import sys
import os
from pprint import pprint
import numpy as np
import copy

def parse_matrix(fname):
    print("Loading data from file: " + fname)
    f = open(fname, 'r')
    entries = []
    row_index = 0
    for line in f:
        columns = line.split()
        for column_index in xrange(len(columns)):
            entries.append((row_index, column_index, int(columns[column_index])))
        row_index += 1

    pprint(entries)
    return entries


if __name__ == "__main__":
    #File is called as script
    #input to script:
    # <sparse matrix_file> <row indices> <column indices>
    matrix_fname = ""
    row_indices = None
    column_indices = None
    #sample size c=r=2
    sample_size = 2

    #Check command line parameters
    if len(sys.argv) < 4:
        script_name = os.path.basename(__file__)
        print "Usage: " + str(script_name) + " <sparse matrix_file> <row_indices> <column_indices>" \
                                             "\n Where row_indices and column_indices are a comma separated list."
        sys.exit(-1)

    #Load command line parameters
    matrix_fname = sys.argv[1]

    row_indices = sys.argv[2].split(',')
    row_indices = [int(x) for x in row_indices]
    print row_indices

    column_indices = sys.argv[3].split(',')
    column_indices = [int(x) for x in column_indices]
    print column_indices

    entries = parse_matrix(matrix_fname)
    sc = SparkContext(appName="CUR decomposition")
    rdd_entries = sc.parallelize(entries)

    r_matrix = []
    print "r_matrix:"
    for row_index in row_indices:
        selected_row = rdd_entries.filter(lambda (r,c,v): r==row_index)
        print selected_row.collect()
        r_matrix.append(sc.parallelize(selected_row.collect()))

    print "c_matrix:"
    c_matrix = []
    for column_index in column_indices:
        selected_column = rdd_entries.filter(lambda (r,c,v): c==column_index)
        print selected_column.collect()
        c_matrix.append(sc.parallelize(selected_column.collect()))

    print("w_matrix")
    w_matrix = []
    for row in r_matrix:
       element = row.filter(lambda (r,c,v): (c in column_indices)).collect()
       print element

    #Compute SVD using numpy...

