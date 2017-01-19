from HTMLParser import HTMLParser
import sys

global myList
myList=[] 

class MyHTMLParser(HTMLParser):

    def handle_starttag(self, tag, attrs):
        global out 
        # Only parse the 'anchor' tag.
        if tag == "a":
           try: 			 
             # Check the list of defined attributes.
             for name, value in attrs:
                 # If href is defined, print it.
                 if name == "href":
                     
                     #out+=[value]
                     
                     globals()["myList"].append(value)  
                     
             
           except:
			   print sys.exc_info() 
         


listOfPages = open('listOfPages.dat')
dic={}
i = 0

linksData = []

titles = open('pages.dat','w')

degree=[]
for line in listOfPages:
  
  ll = len(line)	
  dic[line[0:ll-1]] = i

  linksData += [ [] ]
  degree += [0]
  titles.write(str(i)+':'+line)
  i=i+1
listOfPages.close()  


links2 = open('links_from_to.dat','w')  
links = open('links_to_from.dat','w')  
  
  
  
for page in dic: 	
  pageFrom = dic[page]	
  links2.write(str(pageFrom)+":")
  with open ('www1.lehigh.edu/'+page, "r") as myfile:
     data=myfile.read().replace('\n', '')
     parser = MyHTMLParser()
     myList=[]
     try:
       parser.feed(data)
     except:
	   print sys.exc_info() 
     #print "-----------------------------------"
     #print page
     
     pp = page.split('/')
     pp = pp[1:len(pp)-1] 
     
     for linkTo in myList:
		 
		 if linkTo[0:4]=='http':
			 pass
		 else:
			 linkTo=linkTo.split('#')
			 linkTo=linkTo[0]
			 counttime = linkTo.count('../')
			 prefix = pp[0:len(pp)-counttime]
			 linkTo = linkTo.replace("../", "", counttime)
			 if counttime == 0:
			   finalLink =  './'+'/'.join(prefix)+'/'+linkTo
			 else:
			   finalLink =  '.'+'/'.join(prefix)+'/'+linkTo
			 if dic.has_key(finalLink):
				 pageTo = dic[finalLink]
				 degree[pageFrom]=degree[pageFrom]+1
				 linksData[pageTo] += [pageFrom]
				 links2.write(str(pageTo)+" ")
     links2.write("\n") 
 			   
i = 0			 
for e in linksData:
	myset = set(e)
	e = list(myset)
	e = sorted(e)
	ss="";
	for vv in e:
		val = 0
		if (degree[vv] > 0):
			val=1/(degree[vv]+0.0)
		ss=ss+ str(vv)+"_"+str(val)+" " 
	 
	links.write(str(i)+':'+ss+'\n')
	i=i+1
	   
      


titles.close()
links.close()

