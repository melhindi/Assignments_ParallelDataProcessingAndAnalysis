#!/usr/bin/env python
import numpy
import os
import sys
from pprint import pprint

__author__ = 'developer'

"""
Implement the 'naive' DGIM algorithm ('Exponential Windows', lecture 9) in Python,
Java or Scala. The program should take as input the window width and a large (but
finite) bit array to simulate a stream. There should be a routine to move the window by
one, and another routine to request an estimate for the number of 1's among the k most
recent bits. Test your implementation by creating a bit array of 1000 bits containing
randomly generated bits and a window of size 20. Then move over the bit array and
request the estimate for k = 15 every tenth step. Calculate the relative difference to the
actual value and present it as a table. Generate a plot of all these errors.
"""

class DGIM:
    def __init__(self, window_width, bit_array):
        self.window_width = window_width
        self.bit_array = bit_array
        self.array_length = len(bit_array)
        self.counting_list = []
        self.current_index = -1

    def move_window(self):
        if (self.current_index < self.array_length-1):
            self.current_index += 1
            #print("Reading bit at index %i: %i" % (self.current_index, self.bit_array[self.current_index]))
            #sys.stdout.write("%i " % self.bit_array[self.current_index])
            self.add_bit(self.bit_array[self.current_index], 0)
            self.remove_last_bucket()
            #self.print_counting_list()
            return True
        else:
            #End of array reached
            #sys.stdout.write("\n")
            return False

    def remove_last_bucket(self):
        #drop the last (oldest) bucket if its end-time is prior to N time units before the current time
        levels = len(self.counting_list)
        end_time = 0
        for i in xrange(levels):
            end_time += (2**i)*len(self.counting_list[i])
        #print "end time: %i" % end_time
        if end_time >= self.window_width+2**(levels-1):
            #print "deleting last bucket"
            #self.print_counting_list()
            self.counting_list[levels-1].pop()
            #self.print_counting_list()

    def add_bit(self, bit, level):
        if (len(self.counting_list) > level):
            #There is a bucket
            #Check length of bucket
            if (len(self.counting_list[level]) < 2):
                #The bucket is not full
                self.counting_list[level].append(bit)
                return -1
            else:
                #The bucket is full we need to move to a higher level
                #Add the leftmost two bits
                sum = self.counting_list[level][0]+self.counting_list[level][1]
                #Delete the leftmost two bits
                self.counting_list[level].pop(0)
                self.counting_list[level].pop(0)
                self.counting_list[level].append(bit)
                return self.add_bit(sum, level+1)
        else:
            #There is no bucket, we create a new level and add the bit
            self.counting_list.append([bit])
            return -1

    def estimate_ones(self, k):
        """
        Estimate the number of 1's among the k most recent bits
        :param k: number of recent bits
        """
        reached_end = False
        estimated_ones = 0
        bits = 0
        for i in xrange(len(self.counting_list)):
            if reached_end:
                break
            for j in xrange(len(self.counting_list[i])):
                if reached_end:
                    break
                bits += (2**i)
                if (bits < k):
                    estimated_ones += self.counting_list[i][j]
                elif (bits == k):
                    estimated_ones += self.counting_list[i][j]
                    reached_end = True
                elif not reached_end:
                    estimated_ones += 0.5 * self.counting_list[i][j]
                    reached_end = True

        actual_ones = 0
        if self.current_index >= k:
            for i in xrange(self.current_index-k, self.current_index):
                actual_ones += self.bit_array[i]
        else:
            for i in xrange(0, self.current_index+1):
                actual_ones += self.bit_array[i]

        absolute_error = estimated_ones - actual_ones
        relative_error = absolute_error/float(actual_ones)

        if (relative_error > 0.5):
            print "relative error is above 0.5"
            print "%i \t %i \t %i \t %f" % (actual_ones, estimated_ones, absolute_error, relative_error)
            self.print_counting_list()

        return (actual_ones, estimated_ones, absolute_error, relative_error)


    def print_counting_list(self):
        pprint(self.counting_list)

if __name__ == "__main__":
    #File is called as script
    #input to script
    # <window_width> with of the sliding window
    # <bit_array_length> length of random bit array to generate
    window_width = 20
    bit_array_length = 1000
    k = 15

    #Check mandatory command line parameters
    if len(sys.argv) < 3:
        script_name = os.path.basename(__file__)
        print "Usage: " + str(script_name) + " <window_with> <bit_array_length> [<k most recent bits>]"
        sys.exit(-1)

    #Check optional command line parameters
    if (len(sys.argv)>=4):
        k= int(sys.argv[3])

    window_width = int(sys.argv[1])

    if k > window_width:
        print "k > window_width, please specify a window_width >= %i or set k =< %i" % (k,window_width)
        sys.exit(-1)

    bit_array_length = int(sys.argv[2])

    bit_array = numpy.random.randint(2, size=(bit_array_length,))#[1,0,0,1,0,1,0,1,1,0,0]#

    pprint(bit_array)

    dgmi = DGIM(window_width,bit_array)

    print "index      actual estimated e_abs\te_rel"
    #move = True
    #while move:
    for i in xrange(1,bit_array_length+1):
        move = dgmi.move_window()
        if (i % 10 == 0):
            values = dgmi.estimate_ones(k)
            print(str(i) + "  \t\t%i\t\t%i\t\t%i\t\t%f" % values)


    print("\nExponential Windows at the end")

    dgmi.print_counting_list()