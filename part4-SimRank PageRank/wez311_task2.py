import sys
from pyspark import SparkConf, SparkContext
import re
import numpy as np

# Load and parse the data
def parseToFrom(line):
    data = line.split(":")
    pageTo = int(data[0])

    # check if data[1] is empty
    if not data[1]:
        inLinksNum = 0
    else:
        # use unique links in data[1]
        linkData = set(data[1].split())
        # linkData = data[1].split()
        inLinksNum = len(linkData)
    return [pageTo, inLinksNum]

def parseFromTo(line):
    data = line.split(":")
    pageFrom = int(data[0])

    # check if data[1] is empty
    if not data[1]:
        outLinksNum = 0
    else:
        # use unique links in data[1]
        linkData = set(data[1].split())
        # linkData = data[1].split()
        outLinksNum = len(linkData)
    return [pageFrom, outLinksNum]

def pageList(line):
    data = line.split(":")
    key = int(data[0])
    temp = data[1]
    value = temp[2:-5]
    return [key, value]
if __name__ == '__main__':
    # create spark context, sets app and cluster information
    sc = SparkContext(appName = 'wez311_task2')
    # create distributed data
    inLinkData = sc.textFile('/home/globalscratch/ISE495/hw4/lehighWeb/links_to_from.dat').map(parseToFrom).sortBy(lambda x: x[1], ascending = False)
    # get the maximum number of in-going links
    maxIndex1, maxInLinks = inLinkData.first()
    # convert the page list into a dictionary
    pageDict = sc.textFile('/home/globalscratch/ISE495/hw4/lehighWeb/pages.dat').map(pageList).collectAsMap()

    # save results in file
    file = open('4.2_task2.txt','w')
    print '================================================='
    file.write('The websites with the most in-going links ('+str(maxInLinks)+') are:\n')
    print 'The websites with the most in-going links (%d) are:' % maxInLinks
    zeroInLink = 0
    for key, value in inLinkData.collect():
        if value == maxInLinks:
            file.write("http://www1.lehigh.edu/"+pageDict[key]+'\n')
            print "http://www1.lehigh.edu/"+pageDict[key]
        elif value == 0:
            zeroInLink += 1
    print 'The number of pages with no in-going links is %d.' %zeroInLink
    file.write('The number of pages with no in-going links is '+str(zeroInLink)+'.\n')

    outLinkData = sc.textFile('/home/globalscratch/ISE495/hw4/lehighWeb/links_from_to.dat').map(parseFromTo).sortBy(lambda x: x[1], ascending = False)
    # get the maximum number of out-going links
    maxIndex2, maxOutLinks = outLinkData.first()
    print '================================================='
    file.write('\nThe websites with the most out-going links ('+str(maxOutLinks)+') are:\n')
    print 'The websites with the most out-going links (%d) are:' % maxOutLinks
    zeroOutLink = 0
    for key, value in outLinkData.collect():
        if value == maxOutLinks:
            file.write("http://www1.lehigh.edu/"+pageDict[key]+'\n')
            print "http://www1.lehigh.edu/"+pageDict[key]
        elif value == 0:
            zeroOutLink += 1
    print 'The number of pages with no out-going links is %d.' % zeroOutLink
    file.write('The number of pages with no out-going links is '+str(zeroOutLink)+'.\n')

file.close()

sc.stop()