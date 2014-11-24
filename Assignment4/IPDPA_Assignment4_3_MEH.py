"""
Calculating MinHash values
"""

import sys
import os
import pprint

import numpy as np
from pyspark import SparkContext
import random


def generateTestData(numberOfSets, numberOfElements, N):
    if N < 50:
        raise ValueError("Please choose an N (maxInteger) of min. 50 ")
    if numberOfElements > N:
        raise ValueError("Please choose numberOfElements less than or equals to "+str(N))

    numberToReplace = int(numberOfElements * 0.02)
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


def generateHashParams(K):
    hashParams = []
    p = np.int32(2 ** 31 - 1)
    print ("prime is %i" % p)

    for i in xrange(K):
        a = np.int32(random.randrange(1, np.iinfo(np.int32).max))
        b = np.int32(random.randrange(1, np.iinfo(np.int32).max))
        print ("For hashf %i: a=%i, b=%i" % (i,a,b))
        hashParams.append((a,b,p))

    return hashParams


def computeSig(hParams, N, currentSet):
    l = len(hParams)
    #init signature
    signature = np.zeros(l)
    hs =  []
    for i in xrange(l):
        signature[i] = np.iinfo(np.int32).max
        a,b,p = hParams[i]
        #print hParams[i]
        #print ("h%i: (((%i * x + %i) mod %i) mod %i) + 1)" % (i, a,b, p, N))
        hs.append(lambda x: (((a * x + b) % p) % N) + 1)

    print("Finished init signature")
    for e in currentSet:
        for i in xrange(l):
            a,b,p = hParams[i]
            #print hParams[i]
            #print ("h%i: (((%i * %i + %i) mod %i) mod %i) + 1)" % (i, a,e,b, p, N))
            minhash = hs[i](e)
            #print "Iter %i MinHash %i for int %i" % (i, minhash , e)
            if minhash < signature[i]:
                #print("Replacing %i with %i" % (signature[i], minhash))
                signature[i] = minhash

    #print "Signature: "+ str(signature)
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
    hashParams = sc.broadcast(generateHashParams(K))

    data = sc.parallelize(rdd)
    sig = data.map(lambda x: computeSig(hashParams.value, N, x))
    return sig.collect()

def detIndenticalValues(set1, set2):
    counter = 0
    #print set1
    #print set2
    for e in set1:
        if e in set2:
            counter+=1

    print ("Number of identical values %i" % counter)
    return counter


if __name__ == "__main__":
    K = 30
    N = 10**5
    numberOfSets = 100
    numberOfElements = 20000

    if len(sys.argv) < 2:
        scriptname = os.path.basename(__file__)
        print "Running script with default values. You can specify arguments as follows:"
        print "Usage: " + str(
            scriptname) + " [<#hashfunctions>] [<#maxInt>] [<#sets>] [<#elements>]"

    if len(sys.argv) >= 2:
        K = int(sys.argv[1])
    if len(sys.argv) >= 3:
        N = int(sys.argv[2])
    if len(sys.argv) >= 4:
        numberOfSets = int(sys.argv[3])
    if len(sys.argv) == 5:
        numberOfElements = int(sys.argv[4])

    print ("K= %i" % K)
    print ("N= %i" % N)
    print ("numberOfSets= %i" % numberOfSets)
    print ("numberOfElements= %i" % numberOfElements)

    rdd = generateTestData(numberOfSets, numberOfElements, N)
    output = computeMinHashSig(K, N, rdd)
    #rdd = [np.asarray([1,2,3]),np.asarray([4,3,9])]#,np.asarray([5,6,7]),np.asarray([6,8,10])]
    #output = computeMinHashSig(2, 10, rdd)

    pprint.pprint(output)

    print "Length of first set: %i" % len(output[0])
    print "Similarity is calculated as 'num of identical values' / 'length of first set':"
    for i in range(1,len(output)):
        iv = detIndenticalValues(output[0], output[i])
        similarity = iv / float(len(output[0]))
        print "Similarity for sig of column " + str(0) + " and " + str(i) + " = " + str(similarity)
