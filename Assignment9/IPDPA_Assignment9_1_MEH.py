#!/usr/bin/env bash

__author__ = 'developer'

import sys
import os
from pyspark import SparkContext

def count_vowels(word):
    vowels = 0
    for c in word:
        if c in ["a", "e", "i", "o", "u"]:
            vowels += 1
    return (word, vowels)

def find_vowel_words(input_rdd, k):
    return input_rdd.map(count_vowels).sortBy(lambda x: x[1], ascending=False).take(k)

if __name__ == "__main__":

    fname = None
    k = 10

    #Parse mandatory command line parameters
    if len(sys.argv) < 2:
        #File is called as script
        #input to script
        # <file_name> with of the sliding window
        script_name = os.path.basename(__file__)
        print "Usage: " + str(script_name) + " <file_name> [<num of words to find>]"
        sys.exit(-1)

    fname = sys.argv[1]

    #Parse optional command line parameters
    if len(sys.argv) > 2:
        k = int(sys.argv[2])

    sc = SparkContext(appName="count vowels")
    print "Reading file", fname
    lines = sc.textFile(fname)
    words = lines.flatMap(lambda x: x.split())
    output = find_vowel_words(words, k)
    print(str(k) + " longest words in terms of number of vowels are:")
    print(output)