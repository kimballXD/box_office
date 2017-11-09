# -*- coding: utf-8 -*-
"""
box_office: parsing Taiwan box office PDF file released by Taiwan Film Institute
    - Data source: see http://www.tfi.org.tw/about-publicinfo04.asp
    - Dev Environment: python 2.7.12 build from Anaconda 4.1.1, running on Windows 10
    - PDF conversion tool: use pdf2htmlEX, see https://github.com/coolwanglu/pdf2htmlEX
  
Usage: box.py [-h] [-s] [-a APPEND] [--level LEVEL]
optional arguments:
    -l, --latest-crawl N
        Only crawl and convert latest N number of PDF Files. 
        Use this argument when you data is published and you want keep up-to-update. e.g.: '-l 1'
        '-l 0' means crawl all.
    -s, --skip-crawl 
        Skip crawling PDF file from internet, ALSO skip pdf-to-html conversion step.
        Speed up processing if pdf/html file are already stored in the local. May use after the first run.      
        --skip-crawl CANCELS --latest-crawl
    -a APPEND, --append APPEND
        Specify the file path of supplementing data file, which content will be appended after the parsed data. 
        The file should be a tab-delimited file. See raw\\append.csv for a real example.
    --level LEVEL 
        Logging level of python built-in logging module.
          
Output: 
    - box.xlsx: parsed data. xlsx foramt.
    - box.csv: parsed data. tab-delimited file, utf8 without BOM.
    - flat.csv: intermediate parsing result, for debugging use.

Example Usage:
    python box.py
    python box.py -s -a raw/append.csv --level DEBUG
    
@author: kimballXD@gmail.com
"""
import os
import traceback
import re
import datetime
import subprocess as sp
import shutil
import logging
import glob
import requests
from bs4 import BeautifulSoup as bs
import pandas as pd


def _preprocessing(skip_crawl=False, latest_crawl=0):
    if skip_crawl:
        return glob.glob('raw\\*.html')
    
    # get full box office pdf (for the first page...this part needs to modify when paging mechanism kicks in....
    page=requests.get('http://www.tfi.org.tw/about-publicinfo04.asp')
    pageSoup=bs(page.text, 'lxml')
    uris=['http://www.tfi.org.tw/'+x.get('href')  for x in pageSoup.select('a[href^=viewfile]')]
    paths=['raw\\{}.pdf'.format(x.split('=')[-1]) for x in uris][latest_crawl*-1:]
    
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

def _parse_line_index(elements):
    idxs=[]
    re_idx_country_or_date=u'^(\d{1,3})\s+(\W+)(\d{4}/\d{2}/\d{2})?$'
    re_idx_country_date_and_sth=u'(\d{1,3})\s+(\W+)(\d{4}/\d{2}/\d{2})(\W+)$'
    try:
        for idx,x in enumerate(elements):
            if re.match(u'^(\d{1,3})$', x.text) or re.match(re_idx_country_or_date, x.text) or re.match(re_idx_country_date_and_sth, x.text): #get lineIdx
                idxs.append(idx)
        last=idxs[-1]
        rest=enumerate(elements[last:])
        
        # find the latter part of the data for the last record line
        for idx,x in rest:
            if re.search(u'\d+ \d+ [\d,]+ [\d,]+',x.text):
                idxs.append(idx+last+1)
                break
        return (True, (idxs, rest))
    except Exception as e:
        return (False, (idxs, e))

def _parse_page_num(idxs, rest, fileName):
    for idx, x in rest:
        if re.search('^\*',x.text): #check if there is an annotation line
            logging.warning('Found annotation line (i.e., began with "*") in the end of {}. May cause parsing error.'.format(fileName))
            
        footer=re.search(u'第([\d ]+)頁，共[\d ]+頁', x.text) # get page number from footer
        if footer:
            pageNum=footer.group(1).strip()
            break
    return pageNum

def _parse_line(line):
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
    line_parsed=[lineIdx, country, name, pubDate, pubDays, pubTheaters, tickets, sales]
    return line_parsed

def _parse_page(fileName, page, source, parse, flat, ignore_exc=False):
  #start logging
    pageNo=page.get('data-page-no')
    logging.debug('start parsing {}, page {}'.format(fileName ,pageNo))    
    elements=page.select('div > div > div')
    source[fileName]=elements[0].text

    ##get line index: throw exception
    index_parsed=_parse_line_index(elements)
    if index_parsed[0]:
        idxs, rest = index_parsed[1][0], index_parsed[1][1]
    else:
        logging.error('[Failed] Fail {}, page {} failed on parsing line index!'.format(fileName ,pageNo))
        logging.error('current index list: '+ ','.join(index_parsed[1][0]))
        logging.error('Please check flatten page with _see_page_flat and the index list!')
        raise index_parsed[1][1]

    ##get_page_number
    pageNum=_parse_page_num(idxs, rest, fileName)

    ## "split" lines
    flags=zip(idxs[:-1],idxs[1:])
    lines=[]
    for i,j in flags:
        lines.append(u' '.join([x.text for x in elements[i:j]]))

    ## parse lines:
    idx_parsed=[]
    for line in lines:
        line_parsed=_parse_line(line)
        idx_parsed.append(line_parsed[0])
        parse.append([fileName, pageNum]+line_parsed)

    ## check consecutive idx_parsed: thow excpetion
    idx_parsed=sorted([int(x) for x in idx_parsed])
    gaps=0
    for idx, x in enumerate(idx_parsed):
        if idx==0:
            continue
        elif idx>0 and x-idx_parsed[idx-1]==1:
            continue
        else:
            gaps+=1
    if gaps:
        logging.error('[FAILED] File {}, page {} failed to pass the line index consecutivity check! Gaps: {}'.format(fileName, pageNo, gaps))
        logging.error('Current parsed index list: '+ ','.join([str(x) for x in idx_parsed]))
        logging.error('Please check flatten page with _see_page_flat and the index list!')
        if not ignore_exc:
            raise Exception('Custume Error. Please see logging error messege.')

    ## End of parse_page
    flat.extend([u'{}\t{}\t{}\t{}'.format(fileName, pageNum, idx, x.text) for idx,x in enumerate(elements)])

def _parsing(paths, ignore_exc):
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
        for page in pages:
            _parse_page(fileName, page, source, parse, flat, ignore_exc)
            
    #end parsing
    data=pd.DataFrame(parse,columns=['fileName', 'pageNum', 'lineIdx', 'country', 'name', 'pubDate', 'pubDays', 'pubTheaters', 'tickets', 'sales'])
    with open('flat.csv','wb') as flatfile: # dump flat file for debugging
        flatfile.write((u'\n'.join(flat)).encode('utf8'))
    return data, source
#%%
  
def _see_flat_page(path, page, prnt=True):
    """debuging utilities"""
    with open(path,'rb') as infile:
        soup=bs(infile,'lxml')
    pages=soup.select('div[data-page-no]')
    for x in pages:
        if x.get('data-page-no')!=hex(page).replace('0x',''):
            continue
        else:
            elements=x.select('div > div > div')
            if prnt:
                for idx, y in enumerate(elements):
                    print idx, y.text
            return [x.text for x in elements]    
#%%

def _processing_sup_data(data, sup_data_path, appending=False):   
    logging.debug('length of data before processsing supplementing data {}: {}'.format(os.path.basename(sup_data_path) ,len(data)))
    
    #processing
    sup_data=pd.read_csv(sup_data_path ,encoding='utf',sep='\t', parse_dates=[5], infer_datetime_format=True)
    check_keys=['fileName','pageNum','lineIdx']
    drop_index=data[check_keys].reset_index().merge(sup_data[check_keys], on=check_keys, how='inner')['index'] #filtering records by merge
    data=data[~data.index.isin(drop_index)]
    if appending:
        data=data.append(sup_data)     
    logging.debug('length of data before processsing supplementing data {}: {}'.format(os.path.basename(sup_data_path) ,len(data)))
    return data
        

def main(latest_crawl, skip_crawl, ignore_exc, appending, dropping, level='INFO'):
    eval('logging.basicConfig(level=logging.{})'.format(level.upper()))
    
    # download pdf files and convert to html using pdf2htmlEX
    paths=_preprocessing(skip_crawl, latest_crawl)
    newest_file=sorted(paths)[-1].split('\\')[-1].replace('.html','.pdf')
    
    #parse file
    data, source =_parsing(paths, ignore_exc)

    #formating and output
    ## using pandas perser to change data type
    data.to_excel('box.xlsx',index=False, encoding='utf8')       
    data=pd.read_excel('box.xlsx',index=False, encoding='utf8') 
    ## manually appends the lines which can not be correctly parsed by _parsing, and drop the original data which repaired by append file
    
    for sup_data, act in [[appending, True],[dropping, False]]:
        data=_processing_sup_data(data, sup_data, act)
        
#    # get rid of duplicated data
    data=data.groupby(['name','pubDate']).apply(lambda x:x.sort_values('fileName').iloc[-1,:])
    data=data.reset_index(drop=True)
        
    data['pubDate']=[y.date().isoformat() for y in data.pubDate]
    data['underRanking']=data['fileName'].apply(lambda x:x==newest_file)
    data['source']=data['fileName'].map(source)
    data=data.sort_values('sales', ascending=False)
    data.columns=[u'檔案名稱',u'頁碼',u'行號',u'國別地區',u'中文片名',u'上映日期',u'上映日數',u'上映院數',u'累計銷售票數',u'累計銷售金額',u'統計中',u'資料來源']
    data=data.reindex_axis([u'資料來源',u'檔案名稱',u'頁碼',u'行號',u'國別地區',u'中文片名',u'上映日期',u'上映日數',u'上映院數',u'累計銷售票數',u'累計銷售金額',u'統計中'],axis=1)
    
    # output with two format
    data.to_excel('box.xlsx',index=False, encoding='utf8')    
    data.to_csv('box.csv',index=False, encoding='utf8',sep='\t')
    logging.info('[SUCCESS] finish parsing!')
    logging.info('the number of lines from newest file: {} lines'.format(data[u'統計中'].sum()))
    return data

#data=main(latest_crawl=0, skip_crawl=True, ignore_exc=True, appending='raw/append.csv', dropping='raw/drop.csv',level='INFO')
#%%    
if __name__=='__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-l','--latest-crawl', type=int, default=0)
    parser.add_argument('-s','--skip-crawl',action='store_true', default=False)
    parser.add_argument('-i','--ignore-exc',action='store_true', default=False)
    parser.add_argument('-a','--append', help='file path of supplementing data, must be tab-delimited file')
    parser.add_argument('-d','--drop')
    parser.add_argument('--level', default='INFO')
    args=parser.parse_args()
    main(args.latest_crawl, args.skip_crawl, args.ignore_exc, args.append, args.drop, args.level)
    
