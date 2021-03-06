# box_office

## Summary 概要
  - 解析國家電影中心所公佈的全國電影票房 PDF [月資料](http://www.tfi.org.tw/about-publicinfo04.asp) 與[周資料](http://www.tfi.org.tw/about-publicinfo05.asp)
  - 自動抓取並合併所有公佈的PDF，解析後存成歷史檔（保存 PDF 中的原始資料）與現況檔（保存電影最新一筆資料）。
  - 若只須使用資料，請直接下載下列檔案即可
	* 現況檔 [xlsx](https://github.com/kimballXD/box_office/blob/master/box.xlsx)
	* 現況檔 [csv](https://github.com/kimballXD/box_office/blob/master/box.csv)
	* 歷史檔 [xlsx](https://github.com/kimballXD/box_office/blob/master/box_hist.xlsx)
	* 歷史檔 [csv](https://github.com/kimballXD/box_office/blob/master/box_hist.csv)
  - 最新資料版本： 合併至**全國電影票房截至 2018 年 01/08~01/14 統計資訊**。
  
## Technical Infos 技術資訊
  - Python command line tool, wrote in python 2.7.
  - PDF to html conversion using [`PDF2htmlEX`](https://github.com/coolwanglu/pdf2htmlEX)
  
### Usage 使用方式

box.py [-h] [-l N] [-a APPEND] [-d drop] [--level LEVEL]</br>
* `-l, --latest-crawl N`</br>
	只爬取並解析最新的 N 個 PDF 檔，用於更新資料。 '-l 0' 或留空即為全部爬取。
* UPDATE: -s 選項取消。若不選用 -l 選項，則程式會自動比較本地與線上檔案清單的差異，並下載和解析**僅存在於線上的檔案** (i.e., auto-updating)</br>
  ~~`-s, --skip-crawl`</br>
      跳過爬取 PDF 檔的步驟。PDF 檔必須預先儲存在本地目錄。此選項會*清除 -l* 選項。~~
* `-i, --ignore-exc` </br>
	忽視解析過程中發現的問題，直接輸出資料。*請確定已經解決 error log 中的所有 issue 後再選用此選項。*
* `-a APPEND, --append APPEND`</br>
	「補充紀錄」檔案路徑，解析結果中與「補充紀錄」相同的紀錄會被取代，其餘會直接加入解析結果。檔案格式為 tsv，真實範例檔請見 raw/append.csv。
* `-d DROP, --drop DROP`</br> 
	「刪除紀錄」檔案路徑，解析結果中與「刪除紀錄」相同的紀錄會被刪除。檔案格式為 tsv，真實範例檔請見 raw/drop.csv。    
* `--level LEVEL` </br>
	Logging LEVEL of python built-in logging module. specify in UPPERCASE.
### Example 使用範例
* python box.py 
* python box.py -l 1 
* python box.py -i -a raw/append.csv -d raw/drop.csv

### Issues 已知問題
  - [未處理] 由於格式混亂 + 我弱，原始資料中的「申請人」和「出品」等兩個欄位沒解析出來，有需要的人請加油。
  - [手動處理完成] 若「國名地區」欄位若超過三個中文字（在某些頁面是超過二個中文字）時，在解析過程中會被截斷。截斷的部份會跟後面的「中文片名」欄位連在一起，造成這兩個欄位解析錯誤。目前已經用 hardcode + 「補充紀錄檔」處理完成。
  - [手動處理完成] 在月資料檔中，若資料開頭沒有行號將無法正確解析，也可能造成其他資料的解析錯誤。目前已用合併檔處理完成。

  
  
  
