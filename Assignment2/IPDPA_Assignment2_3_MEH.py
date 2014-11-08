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


def closestPoint(p, centers):
    bestIndex = 0
    closest = float("+inf")
    for i in range(len(centers)):
        tempDist = np.sum((p - centers[i]) ** 2)
        if tempDist < closest:
            closest = tempDist
            bestIndex = i
    return bestIndex


def distanceCentroidsMoved(oldCentroids, newCentroids):
    sum = 0.0
    for index in range(len(oldCentroids)):
        sum += np.sum((oldCentroids[index] - newCentroids[index]) ** 2)
    return sum


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


def centroidsChanged(old, new):
    if old is None or new is None:
        return True
    contained = False
    for p in new:
        contained = any((p == x).all() for x in old)
        if not contained:
            return True

    return False


def determine_wcv(closest, centroids):
    """
    Calculate within-cluster variation as SSE
    SSE is sum of squared distances between each sample in
    cluster Ci and its centroid M i, summed over all clusters

    :type closest: RDD
    :type centroids: list
    :param closest: RDD containing all points and the index of the cluster they belong to
    :param centroids: List of the cluster centroids
    """

    # Input validation
    message = ""
    if centroids is None or not centroids:
        message += "The given centroids list is None or empty!"
    if closest is None or closest.count() == 0:
        message += "The passed RDD 'closest' is None or empty!"
        raise ValueError(message)

    # With map we calculate squared distance and with reduce we calc the sum over all clusters
    wcv = closest.map(lambda (k, v): np.sum((centroids[k] - v[0]) ** 2)).reduce(lambda x, y: x + y)
    print("*WCV: " + str(wcv))


def determine_bcv(centroids):
    """
    Calculate between-cluster variation
    We use sum of squared distances between centroids

    :type centroids: list
    :param centroids: List of centroids as numpy vectors
    :return: bcv as float
    """

    # Input validation
    if centroids is None or not centroids:
        raise ValueError("The given centroids list is None or empty!")

    if len(centroids) <= 1:
        bcv = 0
        print("*BCV: " + str(bcv))
        return bcv

    # Extract index of each centroid from the initial list
    # We will use the index later to detect duplicates
    inClusterList = []
    for x in xrange(len(centroids)):
        inClusterList.append((x, centroids[x]))

    # Create a rdd containing the centroids
    inClusterList = sc.parallelize(inClusterList)
    N = inClusterList.count()
    distances = sc.parallelize([])

    # Calculate pairwise distance of centroids
    for cIndex in range(N):
        # Fetch the centroid values for the current centroid index
        iq, q = inClusterList.filter(lambda (idx, p): cIndex == idx).collect()[0]
        # Calculate distance between current centroid and all others
        partialDistances = inClusterList.map(
            lambda (ip, p): (np.sum((p - q) ** 2), (ip, iq) if ip > iq else (
                iq, ip)))  # We sort the index tuple to be able to identify duplicates

        # Remove 0 distances, this saves some space
        partialDistances = partialDistances.filter(lambda (d, indexes): d > 0)
        distances = distances.union(partialDistances)

    distances = distances.distinct().map(lambda (d, indexes): d)
    # Calculate BCV as sum of all distances
    bcv = distances.reduce(lambda d1, d2: d1 + d2)

    print("*BCV: " + str(bcv))
    return bcv


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