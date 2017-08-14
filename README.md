# box_office

## Summary 概要
解析國家電影中心所公佈的[全國電影票房 PDF](http://www.tfi.org.tw/about-publicinfo04.asp)，存成 csv/ xlsx 以利進一部分析應用
  - python 2 寫的, 詳請請見 box.py 檔案開頭。
  - 每次 run 都會重抓 pdf 解析，若網頁架構沒變，每次都會抓到最新的一份。
  - 若想用資料，請直接下載 box.csv/ box.xlsx 即可
  - ~~我是造了什麼孽才來解析PDF~~ ( pycon 梗
  
  
## Issues 已知問題
  - 由於 pdf 的格式實在太混亂 + 我太弱，「申請人」和「出品」等兩個欄位沒解析出來，有需要的人請加油。
  - pdf 檔案內的第 65 筆資料 《google任務：世界之腦》的「國名」欄位解析錯誤，跟「片名」欄位連在一起了。
    * ~~想不到什麼好解法索性不改~~
