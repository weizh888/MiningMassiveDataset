#!/usr/bin/env python                                                                                                  
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
      string = string.replace('\n','') # replace new line \n in string
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
  print 'The # of %d-shingles are %d.' %(k, len(dic.keys()))
  
  fileName = str(k)+'-shingle.txt'
  newFile = open(fileName, 'w')
  for shingle in sortedDic:
    text = shingle + ': ' + str(dic[shingle])
    # newFile.write(text+'\n')
    newFile.write(shingle+'\n') # print the shingle only, w/o count.
  newFile.close()

  # print emptyLine
  
  f.close()


def main():
  k_shingle(2)
  k_shingle(3)

if __name__ == '__main__':
  main()

