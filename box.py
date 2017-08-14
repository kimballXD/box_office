# -*- coding: utf-8 -*-
"""
Created on Wed Aug 09 13:50:41 2017
parsing Taiwan box office pdf file published by Taiwan Film Institute
    - data source: http://www.tfi.org.tw/about-publicinfo04.asp (using lattest published pdf)
    - enviroment: python 2.7.12 build from Anaconda 4.1.1, running on Windows 7
    - pdf conversion tool: pdf2htmlEX, see https://github.com/coolwanglu/pdf2htmlEX

@author: kimballwu
"""

import re
import datetime
import subprocess as sp
import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
import shutil


#0. get lattest box office pdf
page=requests.get('http://www.tfi.org.tw/about-publicinfo04.asp')
pageSoup=bs(page.text, 'lxml')
index='http://www.tfi.org.tw/'
uri=pageSoup.select('a[href^=viewfile]')[0].get('href')

with open('raw/box.pdf','wb') as out:
    rep=requests.get(index+'/'+uri, stream=True)
    rep.raw.decode_content = True
    shutil.copyfileobj(rep.raw, out)        


#1. covert pdf to html using pdf2htmlEX
sp.check_output('bin\\pdf2htmlEX\\pdf2htmlEX.exe raw\\box.pdf --dest-dir raw', shell=True)


#2. parsing converted html
with open('raw/box.html','rb') as infile:
    soup=bs(infile,'lxml')

pages=soup.select('div[data-page-no]')
parse=[]
for page in pages:
    elements=page.select('div > div > div')
#    #observering html pattern
#    for idx,x in enumerate(elements):
#        print idx, x.text
  
    idxs=[]
    for idx,x in enumerate(elements):
        if re.match(u'^(\d{1,3})$', x.text):
            idxs.append(idx)
    idxs.append(idx-2) #drop last three rows
    
    lines=[]    
    flags=zip(idxs[:-1],idxs[1:])
    for i,j in flags:
        lines.append(u' '.join([x.text for x in elements[i:j]]))

    for x in lines:
        x=re.sub('\s+',' ',x)
        split=re.search('(.+?)(\d{4}/\d{2}/\d{2})',x)
        part=split.group(1).split()
        if part[1] in [u'中華民',u'中國大']: # avoid wrong word segment
            country= u''.join(part[1:3])
            name= u''.join(part[3:])
        else:
            country=part[1]
            name= u''.join(part[2:])
        pubDate=split.group(2)
        pubDate=datetime.datetime.strptime(pubDate, '%Y/%m/%d').date()
        pubDays, pubTheaters, tickets, sales=[y.replace(',','') for y in x.split(' ')[-4:]]
        parse.append([country, name, pubDate, pubDays, pubTheaters, tickets, sales])
  
# 3. fromating and output
data=pd.DataFrame(parse,columns=['country', 'name', 'pubDate', 'pubDays', 'pubTheaters', 'tickets', 'sales'])

data.to_excel('box.xlsx',index=False, encoding='utf8')
data=pd.read_excel('box.xlsx',index=False, encoding='utf8') # using pandas perser to change data type
data['pubDate']=[y.date().isoformat() for y in data.pubDate]
data.columns=[u'國別地區',u'中文片名',u'上映日期',u'上映日數',u'上映院數',u'累計銷售票數',u'累計銷售金額']

data.to_excel('box.xlsx',index=False, encoding='utf8')    
data.to_csv('box.csv',index=False, encoding='utf8')    
    