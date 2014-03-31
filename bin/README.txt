本爬虫系统完全采用MapReduce实现:

测试环境：
Debian 7.0
Hadoop 1.2.1
HBase 0.94-16
tika-app-1.50
HttpClient 4.3 下载网页
HttpParser 2.0  解析链接

包括四个模块：
1、crawlerDriver模块，用于爬取网页
2、optimizerDriver模块，从爬取好的页面中去掉那些url不同但页面相同的页面
3、parserDriver模块，用于解析网页链接
4、mergeDriver模块，利用bloomfilter过滤重复URL
5、crawler模块，统筹全局运行爬虫
6、docHBase模块，将url及其对应的页面数据写入HBase，方便Lucene搜索时提取页面数据进行高亮显示同时提供网页快照（由于数据量太大，Lucene建立索引时不存储页面数据）

可通过设置COUNT变量设置爬取网页的深度，默认为爬取三层，循环以下步骤COUNT次
crawler/url/urlX 存放每层爬取的种子链接，从url0开始,作为crawlerDriver的输入，mergeDriver的输出
crawler/doc/docX 存放crawlerDriver的输出，作为ParserDriver的输入，从doc1开始，对应格式为<url document>，document为对应URL的网页值
crawler/doc_final/ 存放最终结果页面，optimizerDriver的输出
crawler/links/linkX，存放ParserDriver的输出，作为mergeDriver的输入，从link1开始，对应格式为<url list(u)>,list(u)是从url中解析出来的链接列表

PS:2014-3-23 BY zp

1、爬虫模块
 此模块可分为五部分：
   crawlerDriver模块，用于爬取网页
   optimizerDriver模块，从爬取好的页面中去掉那些url不同但页面相同的页面
   parserDriver模块，用于解析网页链接
   mergeDriver模块，利用bloomfilter过滤重复URL
   docHBase模块，将url及其对应的页面数据写入HBase，方便Lucene搜索时提取页面数据进行高亮显示同时提供网页快照（由于数据量太大，Lucene建立索引时不存储页面数据）

可通过设置COUNT变量设置爬取网页的深度，默认为爬取三层，循环以下步骤COUNT次
crawler/url/urlX 存放每层爬取的种子链接，从url0开始,作为crawler的输入，merge的输出
crawler/doc/docX 存放crawler的输出，作为Parser的输入，从doc1开始，对应格式为<url document>，document为对应URL的网页值
crawler/doc_final/ 存放最终结果页面，optimizer的输出
crawler/links/linkX，存放Parser的输出，作为merge的输入，从link1开始

2、建立索引
   从crawler/doc_final中提取数据利用Lucene在HDFS中建立索引

3、检索
   从索引中查询结果并显示在页面上，查询结果包括url，网页摘要，高亮，及网页快照。由于网页内容并没有存入Lucene索引，所以需要从HBase中读取数据显示网页摘要，并提供快照功能。

   相应模块将不定期上传。
