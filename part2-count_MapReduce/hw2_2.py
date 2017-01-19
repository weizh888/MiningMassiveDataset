import re
from pyspark import SparkContext

def add_values(a, b):
    return a+b

def key_words(line):
    words = line.split(" ")
    keyWords = words[2][29:] # 3rd column, only important infomation
    return (keyWords, 1) # a tuple

def get_value(element):
    return element[1]

if __name__=='__main__':
    # create spark context, sets app and cluster information
    sc = SparkContext(appName='hw2_2_URL')
    # create RDD object, creates distributed data
    # rdd0 = sc.textFile('/home/globalscratch/ISE495/hw2/wikipedia_access_log_small.log')
    rdd0 = sc.textFile('/home/globalscratch/ISE495/hw2/wikipedia_access_log_large.log')

    exList=[".jpg",".JPG",".svg",".SVG",".png",".PNG",".asp",".js",".css",".gif",".GIF","title=","monobook",".php",".ico",".ICO","Special:",".txt",".ogg",".dll",".class"] # ".html"
    def excludef(line):
        flag = 0 # flag=1 means at least one element in exList is in that line
        for ext in exList:
            if ext in line:
                flag = 1
                break
        if flag==0:
            return line # only the third column
        else:
            return []

    rdd_en = rdd0.filter(lambda line: "http://en.wikipedia.org" in line).filter(excludef)
    # print rdd_en.first()
            
    # create a new RDD
    rdd2 = rdd_en.map(key_words).reduceByKey(add_values).sortBy(get_value, ascending=False)
	
    file =  open('hw2_2_results.txt','w')
    # part 1: number of total pages
    totalPageInfo = str(rdd_en.count())+ ' pages in total were accessed.\n\n'
    file.write(totalPageInfo)
    
    # part 2: number of different pages
    difPageInfo = str(rdd2.count())+ ' different pages were accessed.\n\n'
    file.write(difPageInfo)
    
    # part 3: top 40 pages
    file.write('The top 50 pages are'+'\n')
    top50 = rdd2.take(50)
    for (word, count) in top50:
        file.write('http://en.wikipedia.org/wiki/'+word+'  '+str(count)+'\n')
    file.close()