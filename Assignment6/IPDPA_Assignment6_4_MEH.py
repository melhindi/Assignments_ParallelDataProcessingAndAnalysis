#!/usr/bin/env python
__author__ = 'developer'
import sys
import os
import numpy as np

from pprint import pprint
from math import sqrt
from random import random

# y = mx +b
# m is slope, b is y-intercept

def computeError(coefficients, points):
    totalError = 0
    N= len(points)
    #iterate over all points
    for i in range(0, N):
        function_result = 0
        for j in xrange(len(coefficients)):
            function_result += coefficients[j] * (points[i][0]**j)
        #print "Function result"
        #print function_result

        #sum up squares of single errors
        try:
            totalError += (points[i][1] - function_result) ** 2
        except :
            print "Number overflow"
            return 1
    return totalError


def sgd(coefficients, points, learningRate):
    degree = len(coefficients)
    gradients = [0]*degree
    new_coefficients = [0]*degree
    old_coefficients = coefficients
    print("Coefficients")
    pprint(coefficients)

    counter = 0
    error = 10

    #stop conditions
    while not(new_coefficients == old_coefficients) and counter < 4000 and error >0.01:
        print "counter %i" % counter
        old_coefficients = new_coefficients
        new_coefficients = []

        #iterate over all points
        for i in xrange(0, len(points)):
            new_coefficients = [0]*degree
            #calculate different gradients
            for j in xrange(0, degree):
                coefficients_sum = 0;
                for d in xrange(0, degree):
                    coefficients_sum += coefficients[d]*(points[i][0]**d)

                gradients[j] = 2 * (coefficients_sum-points[i][1]) * (points[i][0]**j)
                #print "gradient %f" % gradients[j]

                new_coefficients[j] = coefficients[j] - learningRate*gradients[j]

            #make a step after computing each of the partial gradients
            coefficients = new_coefficients
            #print("New coefficients")
            #pprint(coefficients)

            error = computeError(coefficients,points)
            #print "Error"
            #print(error)
        counter +=1

    return coefficients

def fitFunction(points, degree):
    coefficients = []
    #Initialize SGD with random start values
    for i in xrange(0, degree+1):
        coefficients.append(random())

    coefficients = sgd(coefficients, points, 0.0004)
    print "Final result"
    pprint(coefficients)


if __name__ == "__main__":
    degree = 2
    if len(sys.argv) >= 2:
        degree = sys.argv[1]
    """
    #create random coefficients
    coefficients = [0]*(degree+1)
    for i in xrange(0,degree+1):
        coefficients.append(random()*10)

    #generate random sample
    points = []
    for i in xrange(1001):
        y_value = 0
        for j in xrange(0,len(coefficients)):
            y_value += coefficients[j]*i**j
        #introduce noise of +-1
        noisy_y = y_value + (1-random()*2)
        points.append((i,noisy_y))

    pprint(points)
    """

    #Test with exact values
    degree = 2

    points = []
    for i in range(10):
        a = 0.2
        b = 0.2
        c = 0.2
        points.append((i, a*(i**2)+b*i+c))

    print("Points of function")
    pprint(points)

    fitFunction(points, degree)