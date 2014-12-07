#!/usr/bin/env python
# __author__ = 'developer'

import sys
import os

from pyspark import SparkContext

def calc_avg(rdd):
    """
    The average of all the integers

    :param rdd:
    """
    #avg_tupel = rdd.map(lambda x: (int(x), 1)).reduce(lambda (v1,c1),(v2,c2): (v1+v2,c1+c2))
    #avg = avg_tupel[0]/float(avg_tupel[1])
    int_rdd = rdd.map(lambda x: int(x))
    sum = int_rdd.sum()
    count = int_rdd.count()
    avg = sum/float(count)

    return avg

def distinct_list(rdd):
    """
    The same set of integers, but with each integer appearing only once


    """
    distinct_list = rdd.map(lambda x: (x, "")).reduceByKey(lambda x, _: x).map(lambda (x, _): x)

    return distinct_list.collect()

def count_distinct(rdd):
    """
    The count of the number of distinct integers in the input


    """
    return len(distinct_list(rdd))


if __name__ == "__main__":
    if len(sys.argv) < 2:
        script_name = os.path.basename(__file__)
        print "Usage: " + str(script_name) + " <file_name>"
        sys.exit(-1)

    fname = sys.argv[1]

    sc = SparkContext(appName="largeIntList")

    lines = sc.textFile(fname)

    s1 = lines.map(lambda x: (int(x), 1))
    #if list is required
    distinct_list = s1.reduceByKey(lambda x,y: x).map(lambda (x, _): x)
    tuple =  s1.reduceByKey(lambda x,y: x).reduce(lambda (x1,y1), (x2,y2): (x1+x2, y1+y2))
    print tuple
    avg_b = tuple[0]/float(tuple[1])

    print "avg %f" % avg_b


    print "The average of all integers is: %f" % calc_avg(lines)
    d_list = distinct_list(lines)
    print "List of distinc integers: " + str(d_list)
    print "The number of distinct integers is: " + str(count_distinct(lines))

