import re
from pyspark import SparkContext

import matplotlib
# Force matplotlib to not use any Xwindows backend.
matplotlib.use('Agg')
import matplotlib.pyplot as plt

def make_tuple(word):
    '''
    returna tuple of (word,1) for a given word.
    '''
    return (word, 1)

def add_values(a, b):
    '''
    Add two given values.
    '''
    return a+b

def list_words(line):
    '''
    Create a list of words from a given line.
    '''
    myStr = re.sub(r'\W+', ' ', line).lower()
    words = myStr.split()
    return words

def get_value(element):
    '''
    return value of an element
    '''
    return element[1]

if __name__=='__main__':
    # create spark context, sets app and cluster information
    sc = SparkContext(appName='hw2_1_WCount')
    # create RDD object, creates distributed data
    # rdd_object = sc.textFile('/home/globalscratch/ISE495/hw2/wikipedia_tiny.txt')
    rdd_object = sc.textFile('/home/globalscratch/ISE495/hw2/wikipedia_large.txt')
    # create a new RDD
    rdd2 = rdd_object.flatMap(list_words).map(make_tuple).reduceByKey(add_values).sortBy(get_value, ascending=False)
    # print rdd2.take(10)
    
    # save words' counts 
    top1000 = rdd2.take(1000)	
    fileOfFreq =  open('hw2_1_top1000.txt','w')
    for (word, count) in top1000:
        fileOfFreq.write(word+'  '+str(count)+'\n')
    fileOfFreq.close()
    
    # plot f(x)
    fig = plt.figure()
    ax = fig.add_subplot(111)
    ax1 = fig.add_subplot(121)
    ax2 = fig.add_subplot(122)
    
    # Turn off axis lines and ticks of the big subplot
    ax.spines['top'].set_color('none')
    ax.spines['bottom'].set_color('none')
    ax.spines['left'].set_color('none')
    ax.spines['right'].set_color('none')
    ax.tick_params(labelcolor='w', top='off', bottom='off', left='off', right='off')

    x = xrange(1,1001,1)
    y = [ row[1] for row in top1000] # take the counts of top-1000 words
    ax1.semilogy(x, y, '+r')    
    #plt.xlabel('the top x-th word')
    #plt.ylabel('count of the top x-th word')
    #plt.title('Power-Laws')

    ax2.loglog(x, y, '+b')    
    #plt.xlabel('the top x-th word')
    #plt.ylabel('count of the top x-th word')
    #plt.title('Power-Laws')
	
    ax.set_xlabel('the top x-th word')
    ax.set_ylabel('count of the top x-th word')
    ax.set_title('Power-Laws')
    
    # plt.show()
    plt.savefig('hw2_1_top1000.png')

    # # plot all words (not required)
    # numOfWords = rdd2.count()
    # print 'The total number of words are %d.' % numOfWords
    # x = xrange(1,numOfWords+1,1)
    # y = rdd2.values().collect()
    
    # # log x axis
    # plt.semilogx(x, y, '+b')
    # plt.ticklabel_format(axis='y',style='sci',scilimits=(1,4))
    # plt.xlabel('the top x-th word')
    # plt.ylabel('count of the top x-th word')
    # plt.title('Power-Laws')
    # # pyplot.xscale('log')
    # plt.show()
    # plt.savefig('hw2_1_all.png')