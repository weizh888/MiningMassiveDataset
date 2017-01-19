import sys
from pyspark import SparkConf, SparkContext
import numpy as np
import scipy.sparse as sps
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.linalg import SparseVector
from pyspark.mllib.regression import LabeledPoint

# Load and parse the data, we substract "-1" on 2 places just to have it indexed from 0
def parsePoint(line):
    data = line.split(":")
    nodeFrom = int(data[0])-1
    linkData = data[1]
    linksTo = [int(x)-1  for x in linkData.split()]
    totalOutLinks = len(linksTo)
    value = 0
    if totalOutLinks > 0:
        values = [1/(totalOutLinks+0.0) for x in linksTo]
        return LabeledPoint(nodeFrom,SparseVector(totalOutLinks,sorted(linksTo),values))
    else:
        return LabeledPoint(nodeFrom,SparseVector(0,[],[]))

def doMultiplication(labeledPoint):
  w = np.zeros(totalNodes) 
  sparseVector = labeledPoint.features
  if sparseVector.size > 0:
    ri = r[labeledPoint.label]
    value = ri*sparseVector.values[0]
    for rowId in sparseVector.indices:
       if rowId < totalNodes:
         w[rowId] += value
  else:
     w = w + r[labeledPoint.label] * 1/(totalNodes+0.0)
  return w   

if __name__ == "__main__":
    # set up environment
    conf = SparkConf()
    conf.setAppName("4.1-SimRank")
    sc = SparkContext(conf = conf)
    
    linkData = sc.textFile("4.1-LinkData.txt").map(parsePoint)
    totalNodes = linkData.map(lambda a: a.label).reduce(max)
    totalNodes = totalNodes + 1
    print "Total pages ", totalNodes
    
    # create a vector "r" 
    r = np.zeros( totalNodes )
    r = r+1/(totalNodes+0.0)
    
    # choose constant beta
    beta = 0.8
    secondPart = r*0
    secondPart[2] = 1-beta
    
    linkData.cache()  # to have faster computation
    for it in xrange(11): # do just 10 iterations
        print "Iteration ",it
        newdata = linkData.map(doMultiplication)
        firstPart = newdata.reduce(lambda a,b: a+b)
        r = beta*firstPart + secondPart
        print r
        
    # print the TOP pages
    rOrig = r.copy()
    # get the largest pageRanks
    top = 10
    B = np.zeros(top, int)
    for i in xrange(top):
        idx = np.argmax(r)
        B[i] = idx; r[idx] = 0
        
    # open file for data
    dic = {}
    f = open("4.1-Titles.txt")
    i = 0;
    for line in f:
        dic[i] = line
        i = i+1
        if i>totalNodes:
            break;
    
    print "Rank  Node#  SimRank  Author"
    for i in xrange(top):
        if B[i]<=3:
            print i+1, " ", B[i]+1, " ", rOrig[B[i]], " ", dic[B[i]]

sc.stop()