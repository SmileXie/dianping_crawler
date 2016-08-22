# dianping_crawler
大众点评网爬虫，致力于从数据的角度分析福州的美食地图

#数据样本

数据样本来源于大家熟知的“大众点评”，收集了其中7740家福州餐厅的数据。
其中，包含以下特征之一的数据，我认为是无效的数据，予以剔除:
* 点评星级为0
* 人均消费大于3000

#代码依赖
主要使用python3编程，使用shell来启动。
依赖Requests, Beautifulsoup, MySQL Python connector. 数据库用到了MySQL。

#启动代码执行
通过执行start_crawling.sh来启动。
在start_crawling.sh中，启动不同进程来爬取不同地区，是为了避免被防爬虫机制拦截。

#统计分析结果
见 [http://www.jianshu.com/p/7af8bf250b3a](http://www.jianshu.com/p/7af8bf250b3a)
