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
        Use this argument when you data is published and you want keep up-to-update. 
        e.g.: '-l 1', '-l 0' means crawl all.
    -i, --ignore-exc
        Ignore problem founds in the parsing stage. Please confirm you have take care of all problems, 
        then use -i to preceed data output in the NEXT submit.
    -a APPEND, --append APPEND
        Specify the file path of supplementing data file, which content will be appended after the parsed data. 
        The file should be a tab-delimited file. See raw\\append.csv for a real example.
    -d DROP, --drop DROP
        Specify the file path of drop data file, which content appears in the parsed data will be DROPPED.
        The file should be a tab-delimited file. See raw\\drop.csv for a real example.
    --level LEVEL 
        Logging level of python built-in logging module.
          
Output: 
    - box.xlsx: parsed data. xlsx foramt.
    - box.csv: parsed data. tab-delimited file, utf8 without BOM.
    - flat.csv: intermediate parsing result, for debugging use.

Example Usage:
    python box.py -l 1
    python box.py -s -a raw/append.csv -d raw/drop.csv --level DEBUG
    
@author: kimballXD@gmail.com
"""
import os
import re
import datetime
import subprocess as sp
import shutil
import logging
import glob
import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
import datetime


COUNTRY=[u'美國',u'印度',u'泰國',u'中華民國',u'日本',u'南韓',u'法國',u'香港',
         u'西班牙',u'英國',u'俄羅斯',u'匈牙利',u'澳洲',u'丹麥',u'中國大陸',
         u'加拿大',u'德國',u'土耳其',u'奧地利',u'以色列',u'義大利',u'塞爾維亞',
         u'新加坡',u'芬蘭',u'巴拿馬',u'挪威',u'斯洛伐克',u'越南',u'比利時',
         u'羅馬尼亞',u'瑞典',u'瑞士',u'菲律賓',u'英國、伊朗',u'愛爾蘭',u'巴西',
         u'義大利、法國',u'日本、香港',u'義大利、法國、德國',u'印尼',u'波蘭',
         u'黎巴嫩',u'烏克蘭',u'西班牙、英國、德國、希臘',u'紐西蘭',u'葡萄牙',
         u'荷蘭',u'智利',u'拉脫維亞',u'墨西哥',u'捷克']

COLUMN_DICTS={'country':u'國別地區','cur_sales':u'本期銷售金額','cur_theaters':u'本期上映院數',
              'cur_tickets':u'本期銷售票數','end_date':u'統計結束日','fileName':u'檔案名稱',
              'lineIdx':u'行號','max_theaters':u'最大上映院數','name':u'中文片名','pageNum':u'頁碼',
              'pubDate':u'上映日期','pub_days':u'累計上映天數','pub_weeks':u'上映週數','range_type':u'統計週期',
              'start_date':u'統計起始日','title':u'資料來源','total_sales':u'累計銷售金額',
              'total_tickets':u'累計銷售票數','underRanking':u'統計中'}

COLUMN_ORD=[u'資料來源',u'檔案名稱',u'頁碼',u'行號',u'中文片名',u'上映日期',u'國別地區',u'統計起始日',u'統計結束日',u'統計週期',
            u'本期上映院數',u'本期銷售票數',u'本期銷售金額',u'累計上映天數',u'上映週數',u'最大上映院數',u'累計銷售票數',u'累計銷售金額',u'統計中']

RANKING_ORD=[u'資料來源',u'檔案名稱',u'頁碼',u'行號',u'中文片名',u'上映日期',u'國別地區',u'上映週數',u'最大上映院數',u'累計銷售票數',u'累計銷售金額',u'統計中']

MONTH_DATA=[('26.pdf','2016-12-06','2017-01-05'),
            ('27.pdf','2017-01-06','2017-02-05'),
            ('31.pdf','2017-02-06','2017-03-05'),
            ('32.pdf','2017-03-06','2017-04-05'),
            ('33.pdf','2017-04-06','2017-05-05'),
            ('34.pdf','2017-05-06','2017-06-05'),
            ('35.pdf','2017-06-06','2017-07-05'),
            ('36.pdf','2017-07-06','2017-08-05'),
            ('37.pdf','2017-08-06','2017-09-05'),
            ('38.pdf','2017-09-06','2017-10-05'),
            ('39.pdf','2017-10-06','2017-11-05')]

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

def _count_lines(path):
    with open(path,'rb') as infile:
        soup=bs(infile,'lxml')
    texts=soup.text
    return len(re.findall('\d{4}/\d{2}/\d{2}',texts))

def _file_type(fileName):
    """return: (row_num, cur_data)"""
    file_id=int(fileName.split('.')[0])
    if file_id <40:
        return True, False
    elif file_id>=40 and file_id<49:
        return False,True
    elif file_id>=49:
        return True, True
                 
#%%
def _preprocessing(latest_crawl=0):
    
    # get full box office pdf (for the first page...this part needs to modify when paging mechanism kicks in....
    uris=[]
    titles=[]
    for index_url in ['http://www.tfi.org.tw/about-publicinfo04.asp','http://www.tfi.org.tw/about-publicinfo05.asp']:
        page=requests.get(index_url)
        page.encoding='utf8'
        pageSoup=bs(page.text, 'lxml')
        files=[x for x in pageSoup.select('a[href^=viewfile]')]
        uris.extend(['http://www.tfi.org.tw/'+x.get('href') for x in files])
        titles.extend([x.select('img')[0].get('title').strip() for x in files])
    
    ## preparing path
    if not os.path.isdir('raw'):
        os.mkdir('raw')
    paths=['raw\\{}.pdf'.format(x.split('=')[-1]) for x in uris]
    htmls=['raw\\{}.html'.format(x.split('=')[-1]) for x in uris]   
    fileNames=[x.split('\\')[-1] for x in paths]
    
    ## preparing crawling list
    items=zip(uris, paths, fileNames, htmls, titles)
    items=sorted(items, key=lambda x:int(re.search('\d+',x[1]).group(0)))
    if latest_crawl:
        crawling=items[latest_crawl*-1:]
    else:
        pdfs=glob.glob('raw\\*.pdf')
        crawling=[x for x in  items if x[1] not in pdfs]
    logging.debug('crawling: {}'.format(crawling.__str__()))

    ## crawling
    for uri, path, fileName, html, title in crawling:
#        logging.debug('fetching uri= {}, filePath= {}'.format(uri, path))
        with open(path,'wb') as out:
            rep=requests.get(uri, stream=True)
            rep.raw.decode_content = True
            shutil.copyfileobj(rep.raw, out)
                 
    ## covert pdf to html using pdf2htmlEX
    for uri, path, fileName, html, title in crawling:        
        sp.check_call('bin\\pdf2htmlEX\\pdf2htmlEX.exe {} --dest-dir raw'.format(path), shell=True)        
    
    ## hardcode to fix wrong data
    try:
        with open('raw/47.html','rb') as infile:
            con=''.join(infile.readlines()).decode('utf8')    
        con=re.sub(u'>療癒心方向',u'>捷克 療癒心方向',con)
        with open('raw/47.html','wb') as outfile:
            outfile.write('{}'.format(con.encode('utf8')))
    except Exception as e:
        os.remove('raw/47.html')
        os.remove('raw/47.pdf')
        raise e
            # retrun infos    
    return items

def _parse_line_index(elements, file_type):
    row_num, cur_data= file_type
    idxs=[]
    re_idx_country_or_date=u'^(\d{1,3})\s+(\W+)(\d{4}/\d{2}/\d{2})?$'
    re_idx_country_date_and_sth=u'^(\d{1,3})\s+(\W+)(\d{4}/\d{2}/\d{2})(\W+)$'
    try:
        if row_num:
            for idx,x in enumerate(elements):
                if re.match(u'^(\d{1,3})$', x.text) or re.match(re_idx_country_or_date, x.text) or re.match(re_idx_country_date_and_sth, x.text): #get lineIdx
                    idxs.append(idx)    
            last=idxs[-1]
            rest=enumerate(elements[last:])
            for idx,x in rest:
                if not cur_data:
                    if re.search(u'\d+ \d+ [\d,]+ [\d,]+',x.text):
                        idxs.append(idx+last+1)
                        break
                else:
                    if re.search(u'\d+ [\d,]+ [\d,]+ [\d,]+ [\d,]',x.text):
                        idxs.append(idx+last+1)
                        break
        else:
            for idx,x in enumerate(elements):           
                for Y in COUNTRY:
                    if re.match(u'^({}\s.+?|{})$'.format(Y,Y), x.text):
                        idxs.append(idx)
                        break
            last=idxs[-1]
            rest=enumerate(elements[last:])
            for idx,x in rest:
                if re.search(u'\d+ [\d,]+ [\d,]+ [\d,]+ [\d,]',x.text):
                    idxs.append(idx+last+1)
                    break
        return (True, (idxs, rest))
    except Exception as e:
        return (False, (idxs, e))

def _parse_page_num(idxs, rest, fileName):
    annotation=False
    for idx, x in rest:
        if re.search('^\*',x.text): #check if there is an annotation line
            logging.warning('Found annotation line (i.e., began with "*") in the end of {}. May cause parsing error.'.format(fileName))
            annotation=True
        footer=re.search(u'第([\d ]+)頁，共[\d ]+頁', x.text) # get page number from footer
        if footer:
            pageNum=footer.group(1).strip()
            break
    return pageNum, annotation

def _parse_line(line, idx, latest_idx, file_type):
    row_num, cur_data=file_type
    line=re.sub('\s+',' ',line)
    split=re.search('(.+?)(\d{4}/\d{1,2}/\d{1,2})',line)
    pubDate=split.group(2)
    pubDate=datetime.datetime.strptime(pubDate, '%Y/%m/%d').date()
    part=split.group(1).split()
    if row_num:
        lineIdx=part[0]
        if part[1] in [u'中華民',u'中國大',u'中國',u'中華',u'加拿',u'匈牙',u'西班',u'俄羅',u'斯洛',u'塞爾維',u'奧地',u'義大',u'羅馬']: # avoid wrong word segmentaton if possible
            country= u''.join(part[1:3])
            name= u''.join(part[3:])
        else:
            country=part[1]
            name= u''.join(part[2:])
            
        if not cur_data:
            pub_days, max_theaters, total_tickets, total_sales=[x.replace(',','') for x in line.split(' ')[-4:]]
            line_parsed=[lineIdx, country, name, pubDate, pub_days, -1,  max_theaters, -1, -1, total_tickets, total_sales]
        else:
            cur_theaters, cur_tickets, cur_sales, total_tickets, total_sales=[x.replace(',','') for x in line.split(' ')[-5:]]        
            line_parsed=[lineIdx, country, name, pubDate, -1, cur_theaters, -1, cur_tickets, cur_sales, total_tickets, total_sales]
    else:
        lineIdx=latest_idx+idx+1
        country=part[0]
        name=u''.join(part[1:])
        cur_theaters, cur_tickets, cur_sales, total_tickets, total_sales=[x.replace(',','') for x in line.split(' ')[-5:]]        
        line_parsed=[lineIdx, country, name, pubDate, -1, cur_theaters, -1, cur_tickets, cur_sales, total_tickets, total_sales]

    return line_parsed

def _parse_page(fileName, page, source, parse, file_type, flat, latest_idx):
    excs=False
    #start logging
    pageNo=page.get('data-page-no')
    logging.debug('start parsing {}, page {}'.format(fileName ,pageNo))    
    elements=page.select('div > div > div')
    source[fileName]=elements[0].text

    ##get line index: throw exception       
    index_parsed=_parse_line_index(elements, file_type)        
    if index_parsed[0]:
        idxs, rest = index_parsed[1][0], index_parsed[1][1]
    else:
        #absolute failed
        logging.error('[Failed] Fail {}, page {} failed on parsing line index!'.format(fileName ,pageNo))
        logging.error('current index list: '+ ','.join(index_parsed[1][0]))
        raise index_parsed[1][1]

    ##get_page_number
    logging.debug('idx: {}'.format(idxs.__str__()))    
    pageNum, annotaion=_parse_page_num(idxs, rest, fileName)
    if annotaion:
        excs=True

    ## "split" lines
    flags=zip(idxs[:-1],idxs[1:])
    lines=[]
    for i,j in flags:
        lines.append(u' '.join([x.text for x in elements[i:j]]))

    ## parse lines:
    idx_parsed=[]
    for idx, line in enumerate(lines):
        line_parsed=_parse_line(line, idx, latest_idx, file_type)
        idx_parsed.append(line_parsed[0])
        parse.append([fileName, pageNum]+line_parsed)

    ## check consecutive idx_parsed: thow excpetion
    idx_parsed=sorted([int(x) for x in idx_parsed])
    gaps=0
    for idx, x in enumerate(idx_parsed):
        if idx==0 and x-latest_idx==1:
            continue
        elif idx>0 and x-idx_parsed[idx-1]==1:
            continue
        else:
            gaps+=1
    if gaps:
        excs=True
        logging.error('[FAILED] File {}, page {} failed to pass the line index consecutivity check! Gaps: {}'.format(fileName, pageNo, gaps))
        logging.error('last idx of previous page: {}'.format(latest_idx))
        logging.error('Current parsed index list: '+ ','.join([str(x) for x in idx_parsed]))
               
    ## End of parse_page
    flat.extend([u'{}\t{}\t{}\t{}'.format(fileName, pageNum, idx, x.text) for idx,x in enumerate(elements)])
    return excs, idx_parsed[-1]

def _parsing(item_info, ignore_exc):
    flat=[]
    parse=[]
    source=dict()

    #start parsing!
    ## parse file
    job_excs=False
    for uri, path, fileName, html, title in item_info:
        file_type=_file_type(fileName)
        with open(html,'rb') as infile:
            soup=bs(infile,'lxml')
        pages=soup.select('div[data-page-no]')
        latest_idx=0
        for page in pages:
            page_res, latest_idx=_parse_page(fileName, page, source, parse, file_type, flat, latest_idx)
            job_excs= job_excs or page_res
            
    if job_excs:
        if not ignore_exc:
            raise Exception('[Job Stop] Job stopped due to sth. wrong happend. Please the read warning/error messege and take care of all problems, then use -i option to IGNORE exception to proceed to the output of data.')
        else:
            logging.warning('[WARNING] Sth. wrong happend but ignored because ignore-exc option has been used. Please make sure all problem has been managed properly. Proceeding to the output of data.')

    #end parsing
    data=pd.DataFrame(parse,columns=['fileName', 'pageNum', 'lineIdx', 'country', 'name', 'pubDate', 'pub_days', 'cur_theaters', 'max_theaters', 'cur_tickets','cur_sales', 'total_tickets', 'total_sales'])
    with open('flat.csv','wb') as flatfile: # dump flat file for debugging
        flatfile.write((u'\n'.join(flat)).encode('utf8'))
    return data, source

#%%% ----formating and output

def _processing_sup_data(data, sup_data_path, appending=False):              
    logging.debug('length of data before processsing supplementing data {}: {}'.format(os.path.basename(sup_data_path) ,len(data)))    
    #processing
    sup_data=pd.read_csv(sup_data_path ,encoding='utf',sep='\t', parse_dates=[5], infer_datetime_format=True)
    check_keys=['fileName','pageNum','lineIdx']
    #drop data
    drop_index=data[check_keys].reset_index().merge(sup_data[check_keys], on=check_keys, how='inner')['index'] #filtering records by merge
    data=data[~data.index.isin(drop_index)]    
    if appending:          
        data=data.append(sup_data)     
    logging.debug('length of data before processsing supplementing data {}: {}'.format(os.path.basename(sup_data_path) ,len(data)))
    return data
        

def _get_item_detail(row):
    row['range_type']='monthly'
    if pd.isnull(row['start_date']):
        dates=re.findall('\d+',row['title'])
        row['start_date']='{0}-{1}-{2}'.format(*dates)
        row['end_date']='{0}-{3}-{4}'.format(*dates)
        row['range_type']='weekly'
    row['start_date']=datetime.datetime.strptime(row['start_date'], '%Y-%m-%d')
    row['end_date']=datetime.datetime.strptime(row['end_date'], '%Y-%m-%d')
    return row

def _get_pub_weeks(end_date, pubDate):
    a=end_date.isocalendar()[0]-pubDate.isocalendar()[0]
    b=end_date.isocalendar()[1]-pubDate.isocalendar()[1]+1
    return a*52+b

def _unify_data(gData):
    monthly=gData.loc[gData['range_type']=='monthly',:]
    weekly=gData.loc[gData['range_type']=='weekly',:]
    # monthly: cur_tickets, cur_sales, cur_theaters, pub_weeks
    last_idx=-1
    for idx, row in monthly.iterrows():
        if row['index']==gData['index'].min():
            gData.loc[idx, 'cur_theaters']=gData.loc[idx, 'max_theaters']
            gData.loc[idx, 'cur_tickets']=gData.loc[idx, 'total_tickets']
            gData.loc[idx, 'cur_sales']=gData.loc[idx, 'total_sales']
            last_idx=idx
        else:
            gData.loc[idx, 'cur_theaters']=-1
            gData.loc[idx, 'cur_tickets']=gData.loc[idx, 'total_tickets']-gData.loc[last_idx, 'total_tickets']
            gData.loc[idx, 'cur_sales']=gData.loc[idx, 'total_sales']-gData.loc[last_idx, 'total_sales']                    
            last_idx=idx
       
    # all: pub_weeks, max_theaters:
    max_theaters=-1

    last_idx=-1
    lasting=False
    for idx, row in gData.iterrows():
        #max_theaters
        if row['cur_theaters']>max_theaters:
            max_theaters=row['cur_theaters']
        gData.loc[idx,'max_theaters']=max_theaters           
        
        #pub_weeks      
        gData.loc[idx,'pub_weeks']=_get_pub_weeks(row['end_date'], row['pubDate'])
    return gData

def _get_under_ranking(gData, newest_file):
    if gData['fileName'].max()==newest_file:
        gData['underRanking']=True
    else:
        gData['underRanking']=False
    return gData

def main(latest_crawl, ignore_exc, appending, dropping, level='INFO'):
    eval('logging.basicConfig(level=logging.{})'.format(level.upper()))
    
    # download pdf files and convert to html using pdf2htmlEX
    item_info =_preprocessing(latest_crawl)
    
    #parse file
    data, source =_parsing(item_info, ignore_exc)

    #formating and output
    ## using pandas perser to change data type
    data.to_excel('box.xlsx',index=False, encoding='utf8')       
    data=pd.read_excel('box.xlsx',index=False, encoding='utf8') 
 
    ## manually appends the lines which can not be correctly parsed by _parsing, and drop the original data which repaired by append file
    if appending:
        data=_processing_sup_data(data, appending, True)

    ##loggging number of parsed line in every file
    for idx, x in data.groupby('fileName').size().iteritems():
        logging.info('[INFO] Parsed {} lines (including append/drop data) from {}'.format(x, idx))
        
    ## drop duplicated and error data
    if dropping:
        data=_processing_sup_data(data, dropping, False)
      
    ## drop officially duplicated data    
    data=data[~data.fileName.isin(['40.pdf','41.pdf','42.pdf','43.pdf','44.pdf'])]
        
    ## get Item info data
    item_data=pd.DataFrame([(x[2],x[4]) for x in item_info], columns=['fileName','title'])    
    mon_item=pd.DataFrame(MONTH_DATA,columns=['fileName','start_date','end_date'])
    item_data=item_data.merge(mon_item,on='fileName',how='left')
    item_data=item_data.apply(_get_item_detail,axis=1)
    item_data=item_data.sort_values(by='fileName')
    item_data=item_data.reset_index(drop=True)
    item_data['index']=item_data.index.tolist()
    data=data.merge(item_data,on='fileName')
    data=data.sort_values(by=['name','pubDate','index'])    

    # unify data
    data=data.groupby(['name','pubDate']).apply(_unify_data,)
       
    #formatting
    data=data.groupby(['name','pubDate']).apply(_get_under_ranking, newest_file=item_data['fileName'].max())
    for x in ['pubDate','start_date','end_date']:
        data[x]=[y.date().isoformat() for y in data[x]]
    data=data.replace(to_replace=-1, value='NULL' )
 
    #output: history file, ranking file       
    hist=data.sort_values(['fileName', 'pageNum', 'lineIdx'])
    hist=hist.rename_axis(COLUMN_DICTS, axis=1)
    hist=hist.reindex_axis(COLUMN_ORD, axis=1)
    hist.to_excel('hist.xlsx',index=False, encoding='utf8')
    hist.to_csv('hist.csv',index=False, encoding='utf8',sep='\t')

    ranking=data.groupby(['name','pubDate']).apply(lambda gData: gData[gData['index']==gData['index'].max()])
    ranking=ranking.drop(labels='index', axis=1)
    ranking=ranking.sort_values('total_sales', ascending=False)
    ranking=ranking.rename_axis(COLUMN_DICTS, axis=1)
    ranking=ranking.rename_axis({u'最大上映院數':u'上映院數'}, axis=1)
    ranking=ranking.reindex_axis(RANKING_ORD, axis=1)
    ranking.to_excel('box.xlsx',index=False, encoding='utf8')
    ranking.to_csv('box.csv',index=False, encoding='utf8',sep='\t')
   
    #finishing
    logging.info('[SUCCESS] finish parsing!')
    logging.info('the number of lines from newest file: {} lines'.format(ranking[u'統計中'].sum()))
    return data

#test
#data=main(latest_crawl=0, skip_crawl=False, ignore_exc=False, appending='raw/append.csv', dropping='raw/drop.csv', level='INFO')
#%%    
if __name__=='__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-l','--latest-crawl', type=int, default=0)
    parser.add_argument('-i','--ignore-exc',action='store_true', default=False)
    parser.add_argument('-a','--append', help='file path of supplementing data, must be tab-delimited file')
    parser.add_argument('-d','--drop')
    parser.add_argument('--level', default='INFO')
    args=parser.parse_args()
    main(args.latest_crawl, args.ignore_exc, args.append, args.drop, args.level)
    
