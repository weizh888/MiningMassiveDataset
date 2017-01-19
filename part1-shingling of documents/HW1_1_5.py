#!/usr/bin/env python                                                                                                  
"""                                                                            
"""

import re

def k_shingle(k):
  f = open('22776-0.txt', 'rU')
  
  dic = {}
  inputString = f.read()
  string = inputString.replace('\n', ' ').replace('.', ' ') # replace new line \n in string
  # print string[0:500]
  
  s = ""
  for i in xrange(len(string)):
    if string[i].isalpha() or string[i].isdigit() or string[i].isspace():
      s = s + string[i]
  myString = re.sub(r'\W+', ' ', s)
  # print myString[0:500]
  # print len(myString)
  for index in range(len(myString)-k+1):
    key = "" # define an empty string for key
    for j in range(k):
      key = key + myString[index+j]
      # key = myString[index]+myString[index+1]
    if (key in dic):
      dic[key] = dic[key]+1
    else:
      dic[key] = 1
  # print dic

  sortedDic = sorted(dic, key=dic.get, reverse=True)
  # print sortedDic
  print 'The # of %d-shingles is %d.' %(k, len(dic.keys()))
  
  storeSpace = k*len(dic.keys())
  if k == 1:
    f2 = open('1.1.5a_result.txt', 'w')
    f3 = open('1.1.5b_result.txt', 'w')
  else:
    f2 = open('1.1.5a_result.txt', 'a+')
    f3 = open('1.1.5b_result.txt', 'a+')
  text1 = 'The # of ' + str(k) + '-shingles is ' + str(len(dic.keys()))
  f2.write(text1+'\n')
  f2.close()
  
  text2 = 'The space taken for '+str(k)+'-shingles is '+str(storeSpace)+' bytes.'
  f3.write(text2+'\n')
  f3.close()
  
  if k <= 3:
    fileName = str(k)+'-shingle_new5.txt'
    newFile = open(fileName, 'w')
    for shingle in sortedDic:
      text = shingle + ': ' + str(dic[shingle])
      # newFile.write(text+'\n')
      newFile.write(shingle+'\n') # print the shingle only, w/o count.
    newFile.close()

  # print emptyLine
  
  f.close()


def main():
  for k in range(1,11):
    k_shingle(k)

if __name__ == '__main__':
  main()

