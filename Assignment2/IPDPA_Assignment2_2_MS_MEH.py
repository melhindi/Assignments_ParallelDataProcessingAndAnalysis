import matplotlib.pyplot as plt
from scipy.cluster.hierarchy import dendrogram, linkage
from scipy.spatial.distance import pdist
from scipy.spatial.distance import squareform
#from numpy import array
import numpy as np
from numpy.matlib import repmat, repeat

points = [[0, 0], [10, 10], [21, 21], [33, 33],[5, 27],[28, 6]]

print "Data set"
points_mat = np.matrix(points)
print(points_mat)

print "Scipy condensed distance matrix"
dist_mat = pdist(points_mat)
print(dist_mat)

print("Single Linkage")
linkage_matrix = linkage(dist_mat, 'single')
print linkage_matrix

plt.figure("Single Linkage")
plt.title("Single Linkage")
dendrogram(linkage_matrix,
           color_threshold=1,
           truncate_mode='lastp',
           labels=['a', 'b', 'c', 'd', 'e', 'f'],
           distance_sort='descending')

print("Complete Linkage")
linkage_matrix = linkage(dist_mat, 'complete')
print linkage_matrix

plt.figure("Complete Linkage")
plt.title("Complete Linkage")
dendrogram(linkage_matrix,
           color_threshold=1,
           truncate_mode='lastp',
           labels=['a', 'b', 'c', 'd', 'e', 'f'],
           distance_sort='descending')

print("Average Linkage")
linkage_matrix = linkage(dist_mat, 'average')
print linkage_matrix

plt.figure("Average Linkage")
plt.title("Average Linkage")
dendrogram(linkage_matrix,
           color_threshold=1,
           truncate_mode='lastp',
           labels=['a', 'b', 'c', 'd', 'e', 'f'],
           distance_sort='descending')

print("Centroid Linkage")
print "Scipy squareform distance matrix required"
dist_mat = squareform(dist_mat)

print(dist_mat)
linkage_matrix = linkage(dist_mat, 'centroid')
print linkage_matrix

plt.figure("Centroid Linkage")
plt.title("Centroid Linkage")
dendrogram(linkage_matrix,
           color_threshold=1,
           truncate_mode='lastp',
           labels=['a', 'b', 'c', 'd', 'e', 'f'],
           distance_sort='descending')

plt.show()