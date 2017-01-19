from __future__ import print_function
import sys
import re
import numpy as np
from operator import add
from pyspark import SparkContext
from functools import partial


if __name__ == "__main__":
    sc = SparkContext(appName="LHS")
    np.random.seed(10)
    signatures = sc.pickleFile("/home/globalscratch/ISE495/hw3/wiki_l_10")
    #signatures = sc.pickleFile("/home/globalscratch/ISE495/hw3/wiki_l_20")
    #signatures = sc.pickleFile("/home/globalscratch/ISE495/hw3/wiki_s_5")
	
    bands = 1
    rowsPerBand = 200
        
    def doLSH(data):
        key=data[0]
        val=data[1]
        out=[]
        if (len(val)>=rowsPerBand*bands):
            for i in xrange(bands):
                hv =hash(str(val[i*rowsPerBand : (i+1)*rowsPerBand] ))
                out+= [ [hv, key ] ]       
        return out
    
    # define a binomial function for calculating candidate pairs
    def binomialCoeff(n, k):
        result = 1
        for i in range(1, k+1):
            result = result*(n-i+1)/i
        return result
    
    def getSig(data):
        key = data[0]
        val = data[1]
        return (key, val)
    
    LSH = signatures.flatMap(doLSH).groupByKey().filter(lambda data: len(list(data[1]))>1   )
    print("----------  LSH done -------")
    total = 0
    candidatePairs = 0
    file =  open('hw3_1_results.txt','w')
    for e in LSH.collect():
	total = total+1
        print ('-----------------')
        print (e[0])
        for j in e[1]:
            print(j)
            file.write(j+'\n')
 #           for data in signatures.collect():
#                if j==data[0]:
                    #print (data[1])
#                    file.write(str(data[1])+'\n')
#                    break
        file.write('\n')
        candidatePairs = candidatePairs + binomialCoeff(len(e[1]),2)
    print ("Total similar groups "+str(total))
    print ("Total candidate pairs are "+str(candidatePairs))
    file.close()
    #stop Spark content
    sc.stop()


