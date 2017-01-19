#!/usr/bin/env python                                                                                                  
"""                                                                            
"""

import re

def k_shingle(k):
  f = open('22776-0.txt', 'rU')
  dic = {}
  emptyLine = 0
  lineNum = 0
  for line in f:
    lineNum = lineNum+1
    if len(line) <= 1:
      emptyLine = emptyLine+1
    else:
      if lineNum == 1:
        string = line[3:]
        # print string
      else:
        string = line
      string = string.replace('.', ' ')
      s = ""
      for i in xrange(len(string)):
        if string[i].isalpha() or string[i].isdigit() or string[i].isspace():
          s = s + string[i]
      string = re.sub(r'\W+', ' ', s)
      for index in range(len(string)-k+1):
        key = "" # define an empty string for key
        for j in range(k):
          key = key + string[index+j]
          # key = string[index]+string[index+1]
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
    f2 = open('1.1.3_result.txt', 'w')
    f3 = open('1.1.4_result.txt', 'w')
  else:
    f2 = open('1.1.3_result.txt', 'a+')
    f3 = open('1.1.4_result.txt', 'a+')
  text1 = 'The # of ' + str(k) + '-shingles is ' + str(len(dic.keys()))
  f2.write(text1+'\n')
  f2.close()
  
  text2 = 'The space taken for '+str(k)+'-shingles is '+str(storeSpace)+' bytes.'
  f3.write(text2+'\n')
  f3.close()
  
  if k <= 3:
    fileName = str(k)+'-shingle_new.txt'
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
		
  #k_shingle(2)
  #k_shingle(3)
  #k_shingle(4)

if __name__ == '__main__':
  main()

