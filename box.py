# -*- coding: utf-8 -*-
"""
box_office: parsing Taiwan box office pdf file released by Taiwan Film Institute
    - Data source: see http://www.tfi.org.tw/about-publicinfo04.asp
    - Environment: python 2.7.12 build from Anaconda 4.1.1, running on Windows 10
    - PDF conversion tool: use pdf2htmlEX, see https://github.com/coolwanglu/pdf2htmlEX
  
Usage: box.py [-h] [-s] [-a APPEND] [--level LEVEL]
optional arguments:
    -s, --skip-crawl 
        Skipping crawl PDF from the internet, ALSO skip pdf-to-html conversion.
        Speed up processing if pdf/html file are already stored in the local. May use after the first run.      
    -a APPEND, --append APPEND
        Specify the file path of the data file, which content will be appended after the parsed data. 
        The file should be a tab-delimited file. See raw\\append.csv for a real example.
    --level LEVEL 
        Logging level of python built-in logging module.
          
Output: 
    - box.xlsx: parsed data. xlsx foramt.
    - box.csv: parsed data. tab-delimited file, utf8 without BOM.
    - flat.csv: intermediate parsing result, for debugging use.
  
@author: kimballXD@gmail.com
"""

import re
import datetime
import subprocess as sp
import shutil
import logging
import glob
import requests
from bs4 import BeautifulSoup as bs
import pandas as pd


def _preprocessing(skip_crawl=False):
    if skip_crawl:
        return glob.glob('raw\\*.html')
    
    # get full box office pdf (for the first page...this part needs to modify when paging mechanism kicks in....
    page=requests.get('http://www.tfi.org.tw/about-publicinfo04.asp')
    pageSoup=bs(page.text, 'lxml')
    uris=['http://www.tfi.org.tw/'+x.get('href')  for x in pageSoup.select('a[href^=viewfile]')]
    paths=['raw\\{}.pdf'.format(x.split('=')[-1]) for x in uris]    

    items=zip(uris,paths)
    for uri, path in items:
        with open(path,'wb') as out:
            rep=requests.get(uri, stream=True)
            rep.raw.decode_content = True
            shutil.copyfileobj(rep.raw, out)                    
    # covert pdf to html using pdf2htmlEX
    for path in paths:        
        sp.check_call('bin\\pdf2htmlEX\\pdf2htmlEX.exe {} --dest-dir raw'.format(path), shell=True)        
    return paths

def _parsing(paths):
    flat=[]
    parse=[]
    source=dict()
    html_paths=[x.replace('.pdf','.html') for x in paths]
    
    #start parsing!
    ## parse file
    for path in html_paths:
        fileName=path.split('\\')[-1].replace('.html','.pdf')
        with open(path,'rb') as infile:
            soup=bs(infile,'lxml')
        pages=soup.select('div[data-page-no]')
        
        ## parse page
        for page in pages:
            logging.debug('start parsing {}, page {}'.format(fileName ,page.get('data-page-no')))
            elements=page.select('div > div > div')
            source[fileName]=elements[0].text
            
            ## "split" lines
            idxs=[]
            for idx,x in enumerate(elements): 
                if re.match(u'^(\d{1,3})$', x.text) or re.match(u'^(\d{1,3}) ([^\d]+)$', x.text): #get lineIdx
                    idxs.append(idx)
            last=idxs[-1]
            rest=enumerate(elements[last:])
            for idx,x in rest: # find the latter part of the data for the last record line
                if re.search(u'\d+ \d+ [\d,]+ [\d,]+',x.text):
                    idxs.append(idx+last+1)
                    last=idx+last+1
                    break            
            for idx,x in rest: 
                if re.search('^\*',x.text): #check if there is an annotation line
                    logging.warning('Found annotation line (i.e., began with "*") in the end of {}. May cause parsing error.'.format(fileName))
                footer=re.search(u'第([\d ]+)頁，共[\d ]+頁', x.text) # get page number from footer
                if footer: 
                    pageNum=footer.group(1).strip()
                    break
            flags=zip(idxs[:-1],idxs[1:])
            lines=[]
            for i,j in flags:
                lines.append(u' '.join([x.text for x in elements[i:j]]))   
                
            ## parse lines                
            for line in lines:
                line=re.sub('\s+',' ',line)
                split=re.search('(.+?)(\d{4}/\d{1,2}/\d{1,2})',line)
                part=split.group(1).split()
                lineIdx=part[0]
                if part[1] in [u'中華民',u'中國大',u'中國',u'中華',u'加拿',u'匈牙',u'西班',u'俄羅',u'斯洛',u'塞爾維',u'奧地',u'義大',u'羅馬']: # avoid wrong word segmentaton if possible
                    country= u''.join(part[1:3])
                    name= u''.join(part[3:])
                else:
                    country=part[1]
                    name= u''.join(part[2:])
                pubDate=split.group(2)
                pubDate=datetime.datetime.strptime(pubDate, '%Y/%m/%d').date()
                pubDays, pubTheaters, tickets, sales=[x.replace(',','') for x in line.split(' ')[-4:]]           
                parse.append([fileName, pageNum, lineIdx, country, name, pubDate, pubDays, pubTheaters, tickets, sales])
                # logging flat file
                [flat.append(u'{}\t{}\t{}\t{}'.format(fileName, pageNum, idx, x.text)) for idx,x in enumerate(elements)]
        logging.info('End of parsing {}.'.format(fileName))

    #end parsing                
    data=pd.DataFrame(parse,columns=['fileName', 'pageNum', 'lineIdx', 'country', 'name', 'pubDate', 'pubDays', 'pubTheaters', 'tickets', 'sales'])
    with open('flat.csv','wb') as flatfile: # dump flat file for debugging
        flatfile.write((u'\n'.join(flat)).encode('utf8'))
    return data, source

def _see_falt(fileNo, page):
    """debuging utilities"""
    path='raw\\{}.html'.format(fileNo)
    with open(path,'rb') as infile:
        soup=bs(infile,'lxml')
    pages=soup.select('div[data-page-no]')
    for x in pages:
        if x.get('data-page-no')!=hex(page).replace('0x',''):
            continue
        else:
            elements=x.select('div > div > div')        
            for idx, y in enumerate(elements):
                print idx, y.text
            break
#%%

def main(skip_crawl, appending, level):
    eval('logging.basicConfig(level=logging.{})'.format(level.upper()))
    
    # download pdf files and convert to html using pdf2htmlEX
    paths=_preprocessing(skip_crawl)
    newest_file=sorted(paths)[-1].split('\\')[-1].replace('.html','.pdf')
    
    #parse file
    data, source =_parsing(paths)
    
    ## manually appends the lines which can not be correctly parsed by _parsing, data format should follow pdf/html
    if appending:
        append=pd.read_csv('raw\\append.csv',encoding='utf',sep='\t')
        data=data.append(append)     

    #formating and output
    data.to_excel('box.xlsx',index=False, encoding='utf8')       
    data=pd.read_excel('box.xlsx',index=False, encoding='utf8') # using pandas perser to change data type
    data=data.groupby(['name','pubDate']).apply(lambda x:x.sort_values('fileName').iloc[-1,:]) #get rid of duplicated data
    data['pubDate']=[y.date().isoformat() for y in data.pubDate]
    data['underRanking']=data['fileName'].apply(lambda x:x==newest_file)
    data['source']=data['fileName'].map(source)
    data.columns=[u'檔案名稱',u'頁碼',u'行號',u'國別地區',u'中文片名',u'上映日期',u'上映日數',u'上映院數',u'累計銷售票數',u'累計銷售金額',u'統計中',u'資料來源']
    data=data.reindex_axis([u'資料來源',u'檔案名稱',u'頁碼',u'行號',u'國別地區',u'中文片名',u'上映日期',u'上映日數',u'上映院數',u'累計銷售票數',u'累計銷售金額',u'統計中'],axis=1)

    ## output with two format
    data.to_excel('box.xlsx',index=False, encoding='utf8')    
    data.to_csv('box.csv',index=False, encoding='utf8',sep='\t')    
    

if __name__=='__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--skip-crawl',action='store_true', default=False)
    parser.add_argument('-a','--append', help='file path of appending data, must be tab-delimited file')
    parser.add_argument('--level', default='INFO')
    args=parser.parse_args()
    main(args.skip_crawl, args.append, args.level)
    
    
