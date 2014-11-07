__author__ = 'developer'
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
The K-means algorithm written from scratch against PySpark. In practice,
one may prefer to use the KMeans algorithm in MLlib, as shown in
examples/src/main/python/mllib/kmeans.py.

This example requires NumPy (http://www.numpy.org/).
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
    if closest is None or closest.count()==0:
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


if __name__ == "__main__":

    if len(sys.argv) != 5:
        # print >> sys.stderr, "Usage: kmeans <file> <k> <convergeDist>"
        print >> sys.stderr, "Usage: kmeans <file> <maxIterations> <Npoints> <k>"
        exit(-1)

    sc = SparkContext(appName="PythonKMeans")

    fname = sys.argv[1]
    maxIterations = int(sys.argv[2])
    Npoints = int(sys.argv[3])
    K = int(sys.argv[4])
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

    centroids = data.takeSample(False, K, 1)
    newCentroids = None  # At the beginning we have no newCentroids, this will let us start calculating newCentroids
    closest = None

    counter = 0
    # tempDist = 2*convergeDist
    while ((counter < maxIterations) and centroidsChanged(centroids, newCentroids)):
        print "\n### Iteration#: %d" % (counter)
        counter += 1

        # Test if we are in the first iteration
        if None != newCentroids:
            centroids = newCentroids[:]  # We already have computed centroids, save them as the old ones
        else:
            newCentroids = centroids[:]

        # Determine cluster which a point belongs to
        closest = data.map(lambda p: (closestPoint(p, centroids), (p, 1)))

        # For each cluster
        for cIndex in range(K):
            # Keep only points belonging to that cluster
            closestOneCluster = closest.filter(lambda d: d[0] == cIndex).map(lambda d: d[1])
            print "Cluster with index %d has %d points" % (cIndex, closestOneCluster.count())

            # Calculate centroids of the cluster based on the points in it
            sumAndCountOneCluster = closestOneCluster.reduce(lambda (p1, one1), (p2, one2): (p1 + p2, one1 + one2))
            vectorSum = sumAndCountOneCluster[0]
            count = sumAndCountOneCluster[1]
            newCentroids[cIndex] = vectorSum / count

        tempDist = distanceCentroidsMoved(centroids, newCentroids)
        print "*tempDist=%f\n*centroids=%s\n*newCentroids=%s" % (tempDist, str(centroids), str(newCentroids))

    print "\n=== Final centers: " + str(centroids)

    determine_wcv(closest,centroids)
    determine_bcv(centroids)

    sc.stop()
