#!/usr/bin/env python                                                                                                  
"""                                                                            
"""

import re

def find_hapaxes(input_file):
  # f = open('22776-0.txt', 'rU')  
  count = {}  
  lineNum = 0
  for line in input_file:
    lineNum = lineNum + 1
    line = line.replace('.', ' ').replace('-', ' ').replace('_', ' ')
    word_list = line.split()
    for word in word_list:
      word = re.sub(r'\W+', '', word)
      if word == '':
        continue
      word = word.lower()
      if word in count:
        count[word] += 1
      else:
        count[word] = 1
  
  fileOfHapaxes = open('1.2.1_hapaxes.txt','w')
  for word in sorted(count.keys()):
    if count[word] == 1:
      myStr = word
      fileOfHapaxes.write(myStr+'\n')
  fileOfHapaxes.close()
  # print len(count.keys())

  fileOfFreq =  open('1.2.2_count.txt','w')
  fileOfFreq.write('x-th  f(x)\n')
  x = 0
  for num in sorted(count.values(), reverse=True):
    x += 1
    myString = "%d  %d" %(x,num)
    fileOfFreq.write(myString+'\n')
  fileOfFreq.close()
  
  input_file.close()
  return count


def main():
  f = open('22776-0.txt', 'rU')
  hapaxes = find_hapaxes(f)

if __name__ == '__main__':
  main()

