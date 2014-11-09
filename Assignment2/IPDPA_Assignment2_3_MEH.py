"""
Hierarchical Clustering Algorithm
"""

import sys
import os

import numpy as np
from pyspark import SparkContext
import random


def parseVector(line):
    return np.array([float(x) for x in line.split(' ')])

def generateData(N, k):
    """ Generates N 2D points in k clusters
        From http://datasciencelab.wordpress.com/2013/12/12/clustering-with-k-means-in-python/
    """
    n = float(N) / k
    X = []
    for i in range(k):
        c = (random.uniform(-1, 1), random.uniform(-1, 1))
        s = random.uniform(0.05, 0.5)
        x = []
        while len(x) < n:
            a, b = np.array([np.random.normal(c[0], s), np.random.normal(c[1], s)])
            # Continue drawing points from the distribution in the range [-1,1]
            if abs(a) < 1 and abs(b) < 1:
                x.append([a, b])
        X.extend(x)
    X = np.array(X)[:N]
    # Write list to file for later use
    f = open('./dataset_N' + str(N) + '_K' + str(k) + '.txt', 'w')
    for x in X:
        f.write(str(x[0]) + " " + str(x[1]) + '\n')

    f.close();

    return X


def getMin(left, right):
    """
    Find the pair with the smallest distance

    :param left: (D(p,q),(p,q),(ip,iq))
    :param right: (D(p,q),(p,q),(ip,iq))
    :return: triple (D(p,q),(p,q),(ip,iq))
    """
    d0, pair0, indices0 = left
    d1, pair1, indices1 = right
    result = left if d0 <= d1 else right
    return result


def mergeClusters(bestPair):
    """

    :param bestPair: (centroid, ((vectorsum,count),(vectorsum,count)) , id)
    :return triple (centroid, (vectorsum,count), id)
    """
    (centroid, ((vectorsum1,count1),(vectorsum2,count2)) , pair) = bestPair

    newVectorsum = vectorsum1+vectorsum2
    newCount = count1+count2
    newCentroid = newVectorsum/newCount

    return (newCentroid, (newVectorsum,newCount), pair)


def merge(inClusterList):
    """

    :param inClusterList: triple (centroid, (vectorsum,count), id)
    :return:
    """

    outClusterList = None
    outMergedPair = None
    # Calculate pairwise distance of centroids
    distances = sc.parallelize([])
    for element in inClusterList.collect():
        # Fetch the centroid values for the current centroid index
        q, c2, iq = element #inClusterList.filter(lambda (p, c, idx): cIndex == idx).collect()[0]
        # Calculate distance between current centroid and all others
        partialDistances = inClusterList.map(
            lambda (p, c, ip): (np.sum((p - q) ** 2), (c,c2), (ip, iq)))

        # Remove 0 distances, this saves some space
        partialDistances = partialDistances.filter(lambda (d, c, indexes): d > 0)
        #bestPair = partialDistances.reduce(getMin)
        #bestPair = sc.parallelize([bestPair])
        distances = distances.union(partialDistances)#bestPair)

    # Find a pair (p,q) of centroids with the smallest distance (their clusters are P, Q)
    bestPair = distances.reduce(getMin)


    # Compute centroid r of cluster R = P Union Q
    newCentroid = mergeClusters(bestPair)
    index1,index2 = newCentroid[2]
    # outClusterList = (inClusterList \ {p, q) Union {r}
    #remove merged pairs from inClusterList
    outClusterList = inClusterList.filter(lambda (centroid, c, id): not (id == index1 or id==index2))
    # add newCentroid to inClusterList
    outClusterList = outClusterList.union(sc.parallelize([newCentroid]))

    outMergedPair = (index1,index2)

    return (outClusterList, outMergedPair)


if __name__ == "__main__":

    if len(sys.argv) != 4:
        # print >> sys.stderr, "Usage: kmeans <file> <k> <convergeDist>"
        print >> sys.stderr, "Usage: kmeans <file> <Npoints> <k>"
        exit(-1)

    sc = SparkContext(appName="PythonKMeans")

    fname = sys.argv[1]
    Npoints = int(sys.argv[2])
    K = int(sys.argv[3])
    if fname is "" or not os.path.isfile(fname):
        fname = './dataset_N' + str(Npoints) + '_K' + str(K) + '.txt'
    data = None
    if os.path.isfile(fname):
        print("Loading data from file: " + fname)
        lines = sc.textFile(fname)
        data = lines.map(parseVector).cache()
    else:
        print("Generating new data for n=" + str(Npoints) + "and k=" + str(K))
        dataLocal = generateData(Npoints, K)
        data = sc.parallelize(dataLocal)

    print "Number of points: %d" % (data.count())
    print "K: %d" % (K)

    # Each point is a cluster
    inClusterList = data.zipWithIndex() # RDD with pairs (point, point_index)
    inClusterList = inClusterList.map(lambda (p,i): (p, (p,1), i)) # RDD with triple (centroid, (vectorsum,count), id)

    mergedList = []
    counter = 0
    print "Running hierarchical clustering..."
    while inClusterList.count() > 1:
        #print "\n### Iteration#: %d" % (counter)
        outClusterList, outMergedPair = merge(inClusterList)
        #print "Merged"
        #print outMergedPair
        mergedList.append(outMergedPair)
        inClusterList = outClusterList
        counter += 1
        sys.stdout.write('.')


    print "\nMerge Result"
    print mergedList
    sc.stop()