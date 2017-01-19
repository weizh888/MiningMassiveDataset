import sys
from pyspark import SparkConf, SparkContext
import re
import numpy as np
import scipy.sparse as sps
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.linalg import SparseVector
from pyspark.mllib.regression import LabeledPoint

# Load and parse the data
def parsePoint(line):
    data = line.split(":")
    nodeFrom = int(data[0])
    if not data[1]:
        return LabeledPoint(nodeFrom,SparseVector(0,[],[]))
    else:
        linksTo = set([int(x) for x in data[1].split()])
        totalOutLinks = len(linksTo)
        values = [1/(totalOutLinks+0.0) for x in linksTo]
        return LabeledPoint(nodeFrom,SparseVector(totalOutLinks,sorted(linksTo),values))

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

def pageList(line):
    data = line.split(":")
    key = int(data[0])
    temp = data[1]
    value = temp[2:-5]
    return [key, value]

if __name__ == "__main__":
    # set up environment
    conf = SparkConf()
    conf.setAppName("wez311_task3")
    sc = SparkContext(conf = conf)

    linkData = sc.textFile("/home/globalscratch/ISE495/hw4/lehighWeb/links_from_to.dat").map(parsePoint)
    totalNodes = linkData.map(lambda a: a.label).reduce(max)
    totalNodes = totalNodes + 1
    print "Total pages ", totalNodes

    # create a vector "r" 
    r = np.zeros( totalNodes )
    r = r+1/(totalNodes+0.0)

    #choose constant beta  
    beta = 0.8
    secondPart = r*(1-beta)

    linkData.cache()  # to have faster computation
    for it in xrange(21): # we will do just 20 iterations
        print "Iteration ",it
        newdata = linkData.map(doMultiplication)
        firstPart = newdata.reduce(lambda a,b: a+b)
        r = beta*firstPart + secondPart
        print r

    # print the TOP pages
    rOrig = r.copy()
    # get the top 30 pageRanks
    top = 100
    B = np.zeros(top, int)
    for i in xrange(top):
        idx = np.argmax(r)
        B[i] = idx; r[idx] = 0

    # get the page dictionary
    pageDict = sc.textFile("/home/globalscratch/ISE495/hw4/lehighWeb/pages.dat").map(pageList).collectAsMap()

    file = open("4.2_task3.txt",'w')
    file.write("The Top "+str(top)+" pages with page rank for Lehigh web are:\n")
    file.write("Rank  pageRank  Link\n")
    print "Rank  Node#  pageRank  Link"
    for i in xrange(top):
        print i+1, " ", B[i], " ", rOrig[B[i]], " ", "http://www1.lehigh.edu/"+pageDict[B[i]]
        simRank = '%.6f' % rOrig[B[i]]
        file.write(str(i+1)+" "+str(simRank)+" http://www1.lehigh.edu/"+pageDict[B[i]]+"\n")

    # pick one page which is not very popular for task 4
    ind = np.argsort(rOrig) # in ascending order
    pagePick = 1500
    print "The %d-th r of page is %d." %(pagePick, ind[pagePick-1])
file.close()

sc.stop()