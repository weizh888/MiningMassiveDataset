import sys
from pyspark import SparkConf, SparkContext
import re

if __name__=='__main__':
    # create spark context, sets app and cluster information
    sc = SparkContext(appName='wez311_task1')
    # create RDD object, creates distributed data
    rdd1 = sc.textFile('/home/globalscratch/ISE495/hw4/lehighWeb/pages.dat')
    print '================================================='
    print 'The total number of pages in our dataset is %d.' %len(rdd1.collect())

sc.stop()
