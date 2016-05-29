"""
Dianping Crawler 
Author: smilexie1113@gmail.com

Dianping Crawler


"""
import requests
import codecs 
from bs4 import BeautifulSoup
import time

CrawlerOption = {
    'headder_host': 'm.api.dianping.com'
};

class DianpingRestaurant(object):
    
    def __init__(self, name, branch_name, price_text, category):
        self._name = name
        self._branch_name = branch_name
        self._price_text = price_text
        self._category = category
        
    def __str__(self):
        outstr = self._name + " " + self._branch_name + " " + self._price_text + " " + self._category;
        return outstr

class DianpingCrawler(object):
    
    def __init__(self):
        self._restaurant = []
        pass
    
    def get_restaurant_list(self, start):
        sec_time = int(time.time())
        response = CrawlerCommon.get(r"http://m.api.dianping.com/searchshop.json?start=" + str(start) \
                                     + "&range=-1&categoryid=10" \
                                     + r"&sortid=0&locatecityid=14&cityid=14&_=" + str(sec_time))
        json_dict = response.json()
        for list_node in json_dict["list"]:
            res = DianpingRestaurant(list_node["name"], list_node["branchName"], list_node["priceText"], \
                                list_node["categoryName"])
            self._restaurant.append(res)
    
    def print_all_restaurant(self):
        for res in self._restaurant:
            print(res)

class CrawlerCommon(object):
    _session = None
    _last_get_page_fail = False #��һ�ε���get_page��ʧ�ܵ�?
    _my_header = {
        'Connection': 'Keep-Alive',
        'Accept': 'text/html, application/xhtml+xml, */*',
        'Accept-Language': 'zh-CN,zh;q=0.8',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko',
        'Accept-Encoding': 'gzip,deflate,sdch',
        'Host': CrawlerOption['headder_host'],
        'DNT': '1'
    }
    
    def __init__(self):
        pass
    
    @staticmethod
    def session_init():
        CrawlerCommon._session = requests.Session()
    
    @staticmethod
    def get_and_save_page(url, path):
        try:
            response = CrawlerCommon._session.get(url, headers = CrawlerCommon._my_header)
            with codecs.open(path, 'w', response.encoding)  as fp:
                fp.write(response.text)
            return
        except Exception as e:
            print("fail to get " + url + " error info: " + str(e))
            return
            
    @staticmethod
    def get_session():
        return CrawlerCommon._session
    
    @staticmethod
    def get_header():
        return CrawlerCommon._my_header
    
    @staticmethod
    def get(url):
        try_time = 0
        
        while try_time < 5:

            if CrawlerCommon._last_get_page_fail:
                time.sleep(10)
                
            try:
                try_time += 1
                response = CrawlerCommon.get_session().get(url, headers = CrawlerCommon._my_header, timeout = 30)
                return response
            except Exception as e:
                print("fail to get " + url + " error info: " + str(e) + " try_time " + str(try_time))
                CrawlerCommon._last_get_page_fail = True
        else:
            raise
    
def main():
    print("start.\n")
    CrawlerCommon.session_init()
    
    dc = DianpingCrawler();
    dc.get_restaurant_list(start = 0);
    dc.print_all_restaurant()
    
    print("ok\n")


if __name__ == "__main__":    
    main()
