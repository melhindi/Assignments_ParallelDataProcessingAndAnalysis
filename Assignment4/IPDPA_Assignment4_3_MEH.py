"""
Hierarchical Clustering Algorithm
"""

import sys
import os

import numpy as np
from pyspark import SparkContext
import random


def generateTestData(numberOfSets, numberOfElements, N):
    if N < 50:
        raise ValueError("Please choose an N (maxInteger) of min. 50 ")
    if numberOfElements > N:
        raise ValueError("Please choose numberOfElements less than or equals to "+str(N))

    numberToReplace = int(numberOfElements * 1)
    print ("numberToReplace= %i"% numberToReplace)
    rddList = []
    firstSet = np.random.random_integers(N, size=numberOfElements)
    rddList.append(np.unique(firstSet))

    for i in range(1, numberOfSets):
        #Si(for i = 1,...,99) is generated from S(i-1)
        newSet = np.copy(rddList[i - 1])
        # replace random 2% of the elements
        elementsToReplace = np.random.choice(len(newSet) - 1, numberToReplace)
        print ("elementsToReplace: "+str(elementsToReplace))
        newSet[elementsToReplace] = np.random.random_integers(N, size=numberToReplace)
        rddList.append(np.unique(newSet))

    print "Sets with random numbers have been generated!"
    print rddList

    return rddList


def generateHashParams(K, N):
    hashParams = []
    p = np.int32(2 ** 31 - 1)
    print ("prime is %i" % p)

    for i in xrange(K):
        a = np.int32(random.randrange(1, np.iinfo(np.int32).max))
        b = np.int32(random.randrange(1, np.iinfo(np.int32).max))
        print ("For hashf %i: a=%i, b=%i" % (i,a,b))
        hashParams.append((a,b,p))
        #

    return hashParams


def minHash(hParams, N, currentSet):
    a,b,p = hParams
    h = lambda x: (((a * x + b) % p) % N) + 1
    #find smallest value hi(r) for a row r with S(r) = 1
    min_value = -1
    # since the set is sorted we iterate the rows with 1s from small to large
    for i in xrange(1, N):
        #print "Looking for"
        #print i
        #print "in:"
        #print currentSet
        index = np.where(currentSet==i)
        #print index[0]
        if index[0]:
            index = index[0][0]
            #print ("Found row with 1 in index %i, computing minHash!" % index)
            if min_value == -1:
                min_value = h(index)
                #print("Set initial minHash %i" % min_value)
            else:
                value = h(index)
                if value < min_value:
                    print("Found smaller minHash %i" % value)
                    min_value = value

        else:
            #print "Element is not contained"
            continue

    print("minHash: "+str(min_value))
    return min_value


def computeSig(hParams, N, currentSet):
    # hier ist das K doch gar nicht definiert, oder?
    l = len(hParams)
    signature = np.zeros(l)
    for i in xrange(l):
        signature[i] = minHash(hParams[i], N, currentSet)
    return signature


def computeMinHashSig(K, N, rdd):
    """

    :param K: number of random hash functions (i.e., the number of rows of the signature matrix)
    :param N: maximum number of elements in any of the considered sets
    :param rdd: RDD where each record contains one set represented as a sorted list of 32-bit integers from the
                range [1 , . . . , N]
    :return: RDD containing the signature matrix, stored column-wise.
             That is, one record holds the K entries that correspond to the signature of one set
    """
    sc = SparkContext(appName="PythonMinhash")
    # first choose a set of K random hash functions h1,..., hK (described in lecture 5 on slide 33)
    hashParams = sc.broadcast(generateHashParams(K, N))

    data = sc.parallelize(rdd)
    sig = data.map(lambda x: computeSig(hashParams.value, N, x))
    return sig.collect()


if __name__ == "__main__":
    numberOfSets = 10
    numberOfElements = 20000

    if len(sys.argv) < 3:
        scriptname = os.path.basename(__file__)
        print >> sys.stderr, "Usage " + str(
            scriptname) + " <#hashfunctions> <#maxInt> [<#sets>] [<#elements>]"
        exit(-1)

    K = int(sys.argv[1])
    print ("K= %i" % K)
    N = int(sys.argv[2])
    print ("N= %i" % N)
    if len(sys.argv) == 4:
        numberOfSets = int(sys.argv[3])
        print ("numberOfSets= %i" % numberOfSets)
    if len(sys.argv) == 5:
        numberOfElements = int(sys.argv[4])
        print ("numberOfElements= %i" % numberOfElements)

    rdd = generateTestData(numberOfSets, numberOfElements, N)
    output = computeMinHashSig(K, N, rdd)

    print output

    # Vergleiche Signaturen
    #for i in range(len(output)):
    #    similarity = (output[0] == output[i]).sum() / float(len(output[0]))
    #    print "Similarity for sig of column " + str(0) + " and " + str(i) + " = " + str(similarity)
