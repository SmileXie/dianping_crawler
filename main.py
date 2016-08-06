#coding=utf-8

"""
Dianping Crawler 
Author: smilexie1113@gmail.com

Dianping Crawler


"""
import requests
import codecs 
from bs4 import BeautifulSoup
import time
import re
import mysql.connector

DianpingOption = {
    'cityid': 14, #Fuzhou
    'locatecityid': 14, #Fuzhou
    'categoryid': 10, #food
    'stop_threshold': 30 #if restaurant number go beyond this, stop crawling
}

class DianpingRestaurant(object):
    
    def __init__(self, id, name, shop_star, branch_name, price_text, category):
        self._id = id
        self._shop_star = shop_star
        self._name = name
        self._branch_name = branch_name
        self._price_text = price_text
        try:
            self._price_num = int(re.findall(r'\d+', self._price_text)[0]) #字符串中的数字，转成int型
        except Exception as e:
            self._price_num = 0
            
        #self._price_num = int(str(filter(str.isdigit, self._price_text)))
        self._category = category
        self._taste = 0
        self._surroundings = 0
        self._service = 0
        self._lat = 0
        self._lng = 0
        self._analyse_shop_page()
        self._analyse_map()
        
    def __str__(self):
        outstr = "id: " + str(self._id) + " " + self._name + " " + self._branch_name + "\n" \
                    + "\t" + r"Price: " + self._price_text + "\n" \
                    + "\t" + r"Category: " + self._category + "\n" \
                    + "\t" + r"Taste: " + str(self._taste) + " Surroundings: " + str(self._surroundings) + " Service: " + str(self._service) + "\n" \
                    + "\t" + r"Star Point: " + str((float)(self._shop_star) / 10) + "\n" \
                    + "\t" + r"Position: " + str(self._lng) + "," + str(self._lat) + "\n"
        #outstr = "%-20s %-20s %-10s %-15s" % (self._name, self._branch_name, self._price_text, self._category)
        return outstr
    
    def _get_shop_url(self):
        return r"http://m.dianping.com/shop/" + str(self._id)
        
    def _get_shop_map_url(self):
        return r"http://m.dianping.com/shop/" + str(self._id) + r"/map"
        
    def _analyse_shop_page(self):
        #CrawlerCommon.get_and_save_page(r"http://m.dianping.com/shop/" + str(self._id), "test.html")
        response =  CrawlerCommon.get(self._get_shop_url())
        soup = BeautifulSoup(response.text)
        """
        <div class="desc">
            <span>口味:9.1</span>
            <span>环境:8.5</span>
            <span>服务:8.7</span>
        </div>
        """
        
        self._valid = True
        name_soup = soup.find("span", class_="shop-name")
        if name_soup is None:
            print("Fail to analyse shop " + str(self._id));
            self._valid = False
            return;
        
        desc_soup = soup.find("div", class_="desc")
        if desc_soup is None :
            print("Fail to analyse shop " + str(self._id) + r"'s score of tates, surroudings, and service.");
            return;
        
        for score_soup in desc_soup.findAll("span"):
            if u"口味" in score_soup.contents[0]:
                self._taste = float(score_soup.contents[0].split(":")[1])
            elif u"环境" in score_soup.contents[0]:
                self._surroundings = float(score_soup.contents[0].split(":")[1])
            elif u"服务" in score_soup.contents[0]:
                self._service = float(score_soup.contents[0].split(":")[1])
                
    def _analyse_map(self):
        try:
            response =  CrawlerCommon.get(self._get_shop_map_url())
            lines = response.text.splitlines()
            #    lat:26.10688581119624, 按冒号拆分成子串后，把最后的逗号删除
            for line in lines:
                if r"lat:" in line:
                    lat_line = line.split(":")[1][:-1]
                    self._lat = float(lat_line)
                elif r"lng:" in line:
                    lng_line = line.split(":")[1][:-1]
                    self._lng = float(lng_line)
        except Exception as ex:
            print("fail to parse location of restanrant " + str(self._id))
                    
    def is_valid(self):
        return self._valid

    def has_star(self):
        return self._shop_star != 0
            
            
class DianpingDb(object):
    
    def __init__(self, db_name, tb_name):
        conn = mysql.connector.connect(user='root', password='password')
        self._cursor = conn.cursor()
        self._db_name = db_name
        self._cursor.execute('DROP DATABASE ' + db_name)
        self._cursor.execute('CREATE DATABASE ' + db_name)
        self._cursor.execute('USE ' + db_name)
        self._cursor.execute('CREATE TABLE ' + tb_name + ' ' 
                               +      '('
                               +      'id int(32) primary key, '
                               +      'name varchar(32),'
                               +      'branch_name varchar(32),'
                               +      'price int(5),'
                               +      'category varchar(32),'
                               +      'lng float(20, 14),'
                               +      'lat float(20, 14),'
                               +      'star float(2, 1)' 
                               +      ')')
        conn.commit()
        self._cursor.close()
        conn.close()

class DianpingCrawler(object):
    
    def __init__(self):
        self._restaurant = []
        pass
    
    def get_restaurant_list_all(self):
        next_start = 0
        last_start = -1
        while next_start >= 0 and next_start > last_start:
            last_start = next_start
            next_start = self.parse_restaurant_list(next_start)
            if last_start > DianpingOption["stop_threshold"]:
                break
    
    def _get_list_url(self, start):
        sec_time = int(time.time())
        url = r"http://m.api.dianping.com/searchshop.json?start=" + str(start) \
                 + r"&range=-1&categoryid=" + str(DianpingOption['categoryid']) \
                 + r"&sortid=0&locatecityid=" + str(DianpingOption['locatecityid']) \
                 + r"&cityid=" + str(DianpingOption['cityid']) + r"&_=" + str(sec_time)
        return url

    def parse_restaurant_list(self, start):
        
        response = CrawlerCommon.get(self._get_list_url(start))
        json_dict = response.json()
        for list_node in json_dict["list"]:
            res = DianpingRestaurant(list_node["id"], list_node["name"], list_node["shopPower"], list_node["branchName"], \
                                     list_node["priceText"], list_node["categoryName"])
            
            if res.is_valid() and res.has_star():
                self._restaurant.append(res)
            else:
                print("skip restaurant " + list_node["name"] + " " + list_node["branchName"] + " id:" + str(list_node["id"]));
                
        return json_dict["nextStartIndex"]
    
    def sorted_restaurants_by_price(self):
        self._restaurant.sort(key=lambda x:(x._price_num), reverse=True)
    
    def print_all_restaurant(self):
        for res in self._restaurant:
            print(res)
        print("restaurant num " + str(len(self._restaurant)));


CrawlerOption = {
    'headder_host': 'm.api.dianping.com'
};

class CrawlerCommon(object):
    _session = None
    _last_get_page_fail = False 
    _my_header = {
        'Connection': 'Keep-Alive',
        'Accept': 'text/html, application/xhtml+xml, */*',
        'Accept-Language': 'zh-CN,zh;q=0.8',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko',
        'Accept-Encoding': 'gzip,deflate,sdch',
        #'Host': CrawlerOption['headder_host'],
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
                response = CrawlerCommon._session.get(url, headers = CrawlerCommon._my_header, timeout = 30)
                return response
            except Exception as e:
                print("fail to get " + url + " error info: " + str(e) + " try_time " + str(try_time))
                CrawlerCommon._last_get_page_fail = True
        else:
            raise
    
def main():
    print("start.\n")
    CrawlerCommon.session_init()
    db = DianpingDb('DianpingRes', 'ResTable')
    dc = DianpingCrawler();
    dc.get_restaurant_list_all()
    dc.sorted_restaurants_by_price()
    dc.print_all_restaurant()
    
    print("ok\n")


if __name__ == "__main__":    
    main()
