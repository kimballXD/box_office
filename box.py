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
import time

#%% ==constant and utitlities

COLUMN_DICTS={'country':u'國別地區',
              'cur_sales':u'本期銷售金額',
              'cur_theaters':u'本期上映院數',
              'cur_tickets':u'本期銷售票數',
              'end_date':u'統計結束日',
              'fileName':u'檔案名稱',
              'lineIdx':u'行號',
              'max_theaters':u'最大上映院數',
              'name':u'中文片名',
              'pageNum':u'頁碼',
              'pubDate':u'上映日期',
              'pub_days':u'累計上映天數',
              'pub_weeks':u'上映週數',
              'publisher':u'發行商',
              'production':u'製片商',
              'range_type':u'統計週期',
              'start_date':u'統計起始日',
              'title':u'資料來源',
              'total_sales':u'累計銷售金額',
              'total_tickets':u'累計銷售票數',
              'underRanking':u'統計中'}

COLUMN_ORD=[u'資料來源',u'檔案名稱',u'頁碼',u'行號',u'中文片名',u'上映日期',u'國別地區',u'發行商',u'製片商',u'統計起始日',u'統計結束日',u'統計週期',
            u'本期上映院數',u'本期銷售票數',u'本期銷售金額',u'累計上映天數',u'上映週數',u'最大上映院數',u'累計銷售票數',u'累計銷售金額',u'統計中']

RANKING_ORD=[u'資料來源',u'檔案名稱',u'頁碼',u'行號',u'中文片名',u'上映日期',u'國別地區',u'上映週數',u'上映院數',u'累計銷售票數',u'累計銷售金額',u'統計中']

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

NUMBER_OF_LOGICAL_COLUMNS=13 # see docstring of _file_type
  
  
def _file_type(fileName):
    """
    hard-code column info of pdf file
    compatible column templates:
        0  lineIdx
        1  country
        2  name
        3  pubDate
        4  publisher
        5  production
        6  pub_days
        7  cur_theaters
        8  max_theaters
        9  cur_tickets
        10 cur_sales
        11 total_tickets
        12 total_sales
    """
    
    file_id=int(fileName.split('.')[0])
    if file_id <40:
        res= {'ncols':10,   # 9 col + row_idx
              'strt_idx':9,
              'missing_cols':(7,9,10),}
               
    elif file_id>=40 and file_id<49:
        res= {'ncols':10,  # 10 col
                'strt_idx':10,
                'missing_cols':(0,6,8),}

    elif file_id>=49:
        res= {'ncols':11,  # 10 col + row_idx
                'strt_idx':11,
                'missing_cols':(6,8)}
    
    # ad-hoc fix: annotaion lines
    if file_id in [31,35]:
        res['drop_annotation']=True
        
    # ad-hoc fix: missing columns
    if file_id in [47]:
        res['impute_cols']=[{'page_idx':3,'insert':[(80,'')]}]
    elif file_id in [35]:
        res['impute_cols']=[{'page_idx':18,'insert':[(89,'131')]}]

    # ad-hoc fix: missing running headers    
    if file_id in [70,71,72,76,77,78,79,80,82,84]:
        res['no_header']=True
    
    # ad-hoc fix: failed parsing (using append file)
    if file_id in [81,83]:
        res['skip']=True
          
    return res
        
def _see_flat_page(path, page, prnt=True):
    """debuging utilities"""
    with open(path,'rb') as infile:
        soup=bs(infile,'lxml')
    pages=soup.select('page')
    for x in pages:
        if x.get('id')!=str(page):
            continue
        else:
            elements=[y.text for y in x.select('p') if y.text]
            if prnt:
                for idx, y in enumerate(elements):
                    print idx, y
            return elements

def _count_lines(path):
    with open(path,'rb') as infile:
        soup=bs(infile,'lxml')
    texts=soup.text
    return len(re.findall('\d{4}/\d{2}/\d{2}',texts))

              
#%% == crawl and parsing ==

def _get_pdf_file_name(fileId):
    headers={'Referer':'https://www.tfi.org.tw/BoxOfficeBulletin/weekly',
         'User-Agent':'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36',
         }
         
    data = {'id':fileId}
    rep=requests.post('https://www.tfi.org.tw/BoxOfficeBulletin/Open/',
                      headers=headers, data=data, verify=False)
    return rep.json()['Url']

              
def _preprocessing(latest_crawl=None):
    # get full box office pdf (for the first page...this part needs to modify when paging mechanism kicks in....
    ids=[]
    data_list=[]
    for index_url in ['https://www.tfi.org.tw/BoxOfficeBulletin/weekly','https://www.tfi.org.tw/BoxOfficeBulletin/monthly']:
        page=requests.get(index_url, verify=False)
        page.encoding='utf8'
        pageSoup=bs(page.text, 'lxml')
        datas=pageSoup.find_all(attrs={'data-id':True})
        data_list.extend(datas)
        ids.extend([x.get('data-id') for x in datas])
    
    # preparing path    
    paths=['raw\\{}.pdf'.format(x) for x in ids]
    tag_files=['raw\\{}.tag'.format(x) for x in ids]   
    fileNames=['{}.pdf'.format(x) for x in ids]
    titles=[x.select('td')[1].text for x in data_list]
    uris=ids # tmp assign 
    
    # filter crawling list using "latest_crawl"
    items=list(set(zip(uris, paths, fileNames, tag_files, titles)))
    items=sorted(items, key=lambda x:int(re.search('\d+',x[1]).group(0)))
    if latest_crawl is not None:
        crawling=items[latest_crawl*-1:]
    else:
        # skip pdf files which has been crawled    
        pdfs=glob.glob('raw\\*.pdf')
        crawling=[x for x in  items if x[1] not in pdfs]


    # if there is if sth to crawl
    if crawling:
        ## fetch uri for crawling pdf
        for idx, crawl_item in enumerate(crawling):  
            try:
                uri=u'https://www.tfi.org.tw'+_get_pdf_file_name(crawl_item[0])
                crawling[idx]=(uri, crawl_item[1], crawl_item[2], crawl_item[3], crawl_item[4])
                time.sleep(1)
            except Exception as e:
                print '[ERROR] error to fetch url at idx {}: file {}'.format(idx,x)
                print e
    
        logging.debug('crawling: {}'.format(crawling.__str__()))
    
        ## crawling
        for uri, path, fileName, tag_file, title in crawling:
            #logging.debug('fetching uri= {}, filePath= {}'.format(uri, path))
            with open(path,'wb') as out:
                rep=requests.get(uri, stream=True, verify=False)
                rep.raw.decode_content = True
                shutil.copyfileobj(rep.raw, out)
                time.sleep(1)
    
                    
        ## covert crawled pdf to html using pdf2tag
        for uri, path, fileName, tag_files, title in crawling:        
            sp.check_call('bin\\pdf2tag\\pdf2tag.exe {}'.format(path), shell=True)        
    
    return items

def _parse_page(fileName, pages, page_idx, latest_idx):
    #preparins
    file_attr=_file_type(fileName)
    if file_attr.has_key('skip'):
        logging.debug('skip parsing {} as setting in file_type'.format(fileName))
        return ([])
    page=pages[page_idx]
   
    # start logging
    pageNum=page.get('id')
    logging.debug('start parsing {}, page {}'.format(fileName ,pageNum))    
    
    # parse content
    elements=[x.text for x in page.select('p') if x.text] # filter empty lines
    
    ## ad-hoc parse content fix
    if file_attr.has_key('drop_annotation') and page_idx+1==len(pages):
        elements=elements[:-1]
        
    if file_attr.has_key('impute_cols'):
        for impute_dict in file_attr['impute_cols']:           
            if impute_dict['page_idx']==page_idx:
                for insert_idx, c in impute_dict['insert']:
                    elements.insert(insert_idx, c)
                    
    if file_attr.has_key('no_header') and page_idx!=0:
        file_attr['strt_idx']=0

    # split lines
    line_idx = range(file_attr['strt_idx'],len(elements),file_attr['ncols'])
    line_idx.append(line_idx[-1]+file_attr['ncols'])
    line_idx_range = zip(line_idx[:-1], line_idx[1:])
    lines=[]
    for i,j in line_idx_range:
        lines.append([x for x in elements[i:j]])

    # parse line into logical column template
    parsed_lines=[]
    for impute_line_idx, line in enumerate(lines):
        ## initialize
        tmp=[]
        skip=0
        ## start parsing columns
        for i in range(NUMBER_OF_LOGICAL_COLUMNS):
            if i in file_attr['missing_cols']:
                if i==0:
                    # missing row number: automatic impute row numbers
                    tmp.append(latest_idx + (impute_line_idx+1))
                else:
                    tmp.append(-1)                   
                skip += 1
            else:
                tmp.append(line[i-skip])
        ## append to the result list
        parsed_lines.append([fileName, pageNum]+tmp)
              
    # end parsing
    logging.debug('end parsing {}, page {}. Parsed {} lines'.format(fileName ,pageNum, len(parsed_lines)))           
              
    # end of parsing page
    return parsed_lines



def _parsing(item_info):
    #initialize
    parse=[]

    #start parsing!
    for uri, path, fileName, tag_file, title in item_info:
        with open(tag_file,'rb') as infile:
            soup=bs(infile,'lxml')
        pages=soup.select('page')
        latest_idx=0
        for page_idx in range(len(pages)):
            parsed_lines=_parse_page(fileName, pages, page_idx, latest_idx)
            parse.extend(parsed_lines)
    
    ## end parsing
    data=pd.DataFrame(parse,columns=['fileName', 'pageNum', 'lineIdx', 'country', 'name', 'pubDate', 'publisher','production','pub_days', 'cur_theaters', 'max_theaters', 'cur_tickets','cur_sales', 'total_tickets', 'total_sales'])
    return data



#%%% == data formating ==

def _processing_sup_data(data, sup_data_path, appending=False):       
    logging.debug('length of data before processsing supplementing data {}: {}'.format(os.path.basename(sup_data_path) ,len(data)))    
    
    #processing
    sup_data=pd.read_csv(sup_data_path ,encoding='utf8',sep='\t', parse_dates=[5], infer_datetime_format=True)
    check_keys=['fileName','pageNum','lineIdx']
    for x in check_keys:
        sup_data[x]=sup_data[x].astype(str)
        data[x]=data[x].astype(str)
        
    #drop data
    drop_index=data.reset_index().merge(sup_data.loc[:,check_keys], on=check_keys, how='inner')['index'] #filtering records by merge
    data=data[~data.index.isin(drop_index)]
    
    # append back data if specified    
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
    # all: transform pub_weeks, max_theaters; must transform before monthly:
    max_theaters=-1
    for idx, row in gData.iterrows():
        #max_theaters    
        if max([row['cur_theaters'],row['max_theaters']])>max_theaters:
            max_theaters=max([row['cur_theaters'],row['max_theaters']])
        gData.loc[idx,'max_theaters']=max_theaters
        
        #pub_weeks      
        gData.loc[idx,'pub_weeks']=_get_pub_weeks(row['end_date'], row['pubDate'])    
        
    # monthly: manage cur_tickets, cur_sales, cur_theaters, pub_weeks
    monthly=gData.loc[gData['range_type']=='monthly',:]
    last_idx=-1
    for idx, row in monthly.iterrows():
        if row['fileID']==gData['fileID'].min():
            gData.loc[idx, 'cur_theaters']=gData.loc[idx, 'max_theaters']
            gData.loc[idx, 'cur_tickets']=gData.loc[idx, 'total_tickets']
            gData.loc[idx, 'cur_sales']=gData.loc[idx, 'total_sales']
            last_idx=idx
        else:
            gData.loc[idx, 'cur_theaters']=-1
            gData.loc[idx, 'cur_tickets']=gData.loc[idx, 'total_tickets']-gData.loc[last_idx, 'total_tickets']
            gData.loc[idx, 'cur_sales']=gData.loc[idx, 'total_sales']-gData.loc[last_idx, 'total_sales']                    
            last_idx=idx
        
    return gData

def _get_under_ranking(gData, newest_file):
    if gData['fileID'].max()==newest_file:
        gData['underRanking']=True
    else:
        gData['underRanking']=False
    return gData

#%% == main process ==

def main(latest_crawl, appending, dropping, level='INFO'):
    eval('logging.basicConfig(level=logging.{})'.format(level.upper()))
    
    # download pdf files and convert to markup file
    item_info =_preprocessing(latest_crawl)
  
    # parse markup file
    data =_parsing(item_info)

    ## manually lines recorded in append file and drop the false-parsed original data  
    if appending:
        data=_processing_sup_data(data, appending, True)

    ## loggging number of parsed line in every file
    for fileName, x in data.groupby('fileName').size().iteritems():
        count_res=_count_lines('raw\\{}'.format(fileName.replace('pdf','tag')))
        if count_res==x:
            logging.info('[INFO] Successfully Parsed {} lines (including append/drop data) from {}'.format(x, fileName))
        else:
            logging.warning('[WARN] Parsed {} lines (including append/drop data) from {}. Inconsistent with result of countline {} lines.'.format(x, fileName, count_res))
        
    ## drop duplicated and error data recorded in dropping file
    if dropping:
        data=_processing_sup_data(data, dropping, False)

    ## dump temparay output (before imputation)
    data.to_csv('box_temp.csv',index=False, encoding='utf8',sep='\t')       
   
    ## drop officially duplicated data    
    data=data[~data.fileName.isin(['40.pdf','41.pdf','42.pdf','43.pdf','44.pdf'])]
        
    ## get Item info data
    item_data=pd.DataFrame([(x[2],x[4]) for x in item_info], columns=['fileName','title'])    
    mon_item=pd.DataFrame(MONTH_DATA,columns=['fileName','start_date','end_date'])
    item_data=item_data.merge(mon_item,on='fileName',how='left')
    item_data=item_data.apply(_get_item_detail,axis=1)
    item_data['fileID']=[int(x.split('.')[0]) for x in item_data['fileName']]
    data=data.merge(item_data,on='fileName')

    ## unify monthy and weekly data
    data=data.groupby(['name','pubDate']).apply(_get_under_ranking, newest_file=item_data['fileID'].max())
      
    ## col/value formatting
    data['pubDate']=data['pubDate'].apply(lambda x: datetime.datetime.strptime(x,'%Y/%m/%d') if not isinstance(x,datetime.datetime) else x)
    for x in ['cur_theaters','max_theaters','cur_tickets','cur_sales','total_tickets','total_sales']:
        data[x]=[int(str(y).replace(',','')) for y in data[x]]
    data['name']=data['name'].str.strip()

    ## unify data
    data=data.groupby(['name','pubDate']).apply(_unify_data,)
     
    ## value formating take II
    data=data.replace(to_replace=-1, value='NULL' )
    for x in ['pubDate','start_date','end_date']:
        data[x]= data[x].apply(lambda y:y.date().isoformat())

    # file output: history file, ranking file       
    hist=data.sort_values(['fileName', 'pageNum', 'lineIdx'])
    hist=hist.rename_axis(COLUMN_DICTS, axis=1)
    hist=hist.reindex_axis(COLUMN_ORD, axis=1)
    hist.to_excel('box_hist.xlsx',index=False, encoding='utf8')
    hist.to_csv('box_hist.csv',index=False, encoding='utf8',sep='\t')

    ranking=data.groupby(['name','pubDate']).apply(lambda gData: gData[gData['fileID']==gData['fileID'].max()])
    ranking=ranking.drop(labels='fileID', axis=1)
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
    parser.add_argument('-l','--latest-crawl', type=int, default=None)
    parser.add_argument('-a','--append', help='file path of supplementing data, must be tab-delimited file')
    parser.add_argument('-d','--drop')
    parser.add_argument('--level', default='INFO')
    args=parser.parse_args()
    main(args.latest_crawl, args.append, args.drop, args.level)
    
