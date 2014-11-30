#!/usr/bin/env python
import sys
import os
import pprint

import numpy as np
from pyspark import SparkContext

def parse_alias_lookup(line):
    bad, good = line.strip().split("\t")
    return (int(bad), int(good))

def create_alias_lookup_table(sc):
    fname = "artist_alias.txt"
    lines = sc.textFile(fname)
    return lines.map(lambda l: parse_alias_lookup(l)).collectAsMap()


"""
Populate a utility matrix. Be sure to first replace bad artist ids that are due to known
misspellings using the assignments in artist_alias.txt. Think about how to store
the matrix in a reasonable way
"""
def split_by_user(line,artist_alias):
    """
    Parse given line and return values as int.

    :param line: A line of the user_artist_data dataset
    :param artist_alias: The look-up table to look for bad artist ids
    :return: tupel containing (userId, {artistId: count})
    """

    #Parse line by splitting on whitespace
    k,a,c = line.strip().split()
    #Convert to int
    k = int(k)
    a = int(a)
    c = int(c)
    #Check for bad artist ids
    if a in artist_alias:
    #Bad artist id found, replace it
        a = artist_alias[a]

    #return parsed line
    return (k, {a:c})

def populate_utility_matrix(sc):
    """
    Loads the user-item matrix from 'user_artist_data.txt'.
    The matrix is stored as a RDD where each record corresponds to a row in the matrix.
    For each record only the artists to which the user has listened to are stored -> we only store 1s

    :param sc: SparkContext to use for task execution
    """

    fname = "user_artist_data.txt"
    #Make look-up table available on all nodes
    artist_alias = sc.broadcast(create_alias_lookup_table(sc))
    #Read dataset
    lines = sc.textFile(fname)
    #Create utility matrix
    utility_matrix = lines.map(lambda line: split_by_user(line, artist_alias.value)).reduceByKey(lambda v1,v2: dict(v1.items() + v2.items()))
    return utility_matrix

"""
Select a suitable distance measure for users (of your choice) and describe briey yourutility_matrix
choice. Implement a routine that calculates this distance measure
"""

def calculate_distance(user1, user2, utility_matrix):
    users = utility_matrix.filter(lambda x: x[0] == user1 or x[0] == user2)
    intersection = 0
    record1, record2 = users.collect()
    l1 = len(record1[1])
    print "length first dict: %i" % l1
    l2 = len(record2[1])
    print "length second dict: %i" % l2
    if l1 < l2:
        for key in record1[1]:
            if key in record2[1]:
                intersection += 1
    else:
        for key in record2[1]:
            if key in record1[1]:
                intersection += 1

    print "intersection: %i" % intersection
    union = l1-intersection + l2-intersection
    print "union: %i" % union
    return float(intersection)/union


if __name__ == "__main__":
    k = 5
    user1 = None
    user2 = None

    if len(sys.argv) < 2:
        scriptname = os.path.basename(__file__)
        print "Running script with default values. You can specify arguments as follows:"
        print "Usage: " + str(
            scriptname) + " <k-nn> <userId> <userId>"

    if len(sys.argv) >= 3:
        k = int(sys.argv[1])
        user1 = int(sys.argv[2])
        print ("k= %i" % k)
        print ("user1= %i" % user1)
    if len(sys.argv) >= 4:
        user2 = int(sys.argv[3])
        print ("user2= %i" % user2)

    sc = SparkContext(appName="PythonRecSys")
    utility_matrix = populate_utility_matrix(sc)
    d = calculate_distance(1000002, 9875, utility_matrix)
    print d


