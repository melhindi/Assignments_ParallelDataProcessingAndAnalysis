from pyspark import SparkContext


if __name__ == "__main__":
    sc = SparkContext(appName="Ex3")
    #create RDD
    nums = sc.parallelize([1,2,3])
    #Actions
    print "!!!!!!Examples for Actions!!!!!"
    #Get content of RDD
    list = nums.collect()
    print "Action  'collect': "+str(list)
    #Show first k elements
    list = nums.take(2)
    print "Action 'take': "+str(list)
    #Count no of elements
    no = nums.count()
    print "Action 'count': "+str(no)

    #Transformations
    print "!!!!!!!Examples for Transformations!!!!!"
    nums = sc.parallelize([1,2,2,3,3,4])
    list = nums.collect()
    print "List with duplicates: "+str(list)
    #Remove duplicates
    noDuplicates = nums.distinct()
    print "List without duplicates: "+str(noDuplicates.collect())
    #Randomly selects a fraction of the items of a RDD and returns them in a new RDD
    sampledRDD = nums.sample(False, 0.1, 1)
    print "Original RDD: "+str(list)
    print "Sampled RDD: "+ str(sampledRDD.collect())
    rdd1 = sc.parallelize([1, 10, 2, 3, 4, 5])
    rdd2 = sc.parallelize([1, 6, 2, 3, 7, 8])
    print "Intersection of two RDDs: "+ str(rdd1.intersection(rdd2).collect())
