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
import os
import sys
import mysql.connector
from mysql.connector import errorcode

DianpingOption = {
    'cityid': 14, #Fuzhou
    'locatecityid': 14, #Fuzhou
    'categoryid': 10, #food
    'stop_threshold': 800, #if restaurant number in one region go beyond this, stop crawling
    'regionids': [0, 98, 100, 99, 101, 27667, 4101, 27670, -986, -987, -984, -433, -983, -981] 
    #全市 鼓楼区 台江区 晋安区 仓山区 闽侯县 马尾区 闽清县 永泰县 平潭县 罗源县 福清 连江 长乐
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
        self._district = ""
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
        
        try:
            for score_soup in desc_soup.findAll("span"):
                if u"口味" in score_soup.contents[0]:
                    self._taste = float(score_soup.contents[0].split(":")[1])
                elif u"环境" in score_soup.contents[0]:
                    self._surroundings = float(score_soup.contents[0].split(":")[1])
                elif u"服务" in score_soup.contents[0]:
                    self._service = float(score_soup.contents[0].split(":")[1])
        except Exception as ex:
            print("Fail to analyse shop " + str(self._id) + r"'s score of tates, surroudings, and service. string: " \
                + str(desc_soup))
            return
        
        district_soup = soup.find("meta", property=r"og:description")
        if district_soup is None:
            return 
        self._district = district_soup["content"].split(" ")[0]
        
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

    def is_reasonable_data(self):
        return self._shop_star != 0 and self._price_num < 3000
    
    def get_db_format(self):
        db_str = [str(self._id), self._name, self._branch_name, str(self._price_num), str((float)(self._shop_star) / 10), \
                    str(self._taste), str(self._surroundings), str(self._service), self._category, \
                    str(self._lng), str(self._lat), self._district]
        return db_str
            
class DianpingDb(object):
    
    def __init__(self, db_name, tb_name, is_create):
        conn = mysql.connector.connect(user='root', password='password')
        self._conn = conn
        self._cursor = conn.cursor()
        self._db_name = db_name
        self._tb_name = tb_name
        if is_create:
            try:
                self._cursor.execute('DROP DATABASE ' + db_name)
            except Exception as ex:
                print("Fail to drop database " + db_name)
            self._cursor.execute('CREATE DATABASE ' + db_name)
        
        self._cursor.execute('USE ' + db_name)
        
        if is_create:
            self._cursor.execute('CREATE TABLE ' + tb_name + ' ' 
                                   +      '('
                                   +      'id int(32) primary key, '
                                   +      'name varchar(20),'
                                   +      'branch_name varchar(16),'
                                   +      'price int(5),'
                                   +      'star float(2, 1),' 
                                   +      'taste float(2, 1),'
                                   +      'surroundings float(2, 1),'
                                   +      'service float(2, 1),'
                                   +      'category varchar(32),'
                                   +      'longitude float(20, 14),'
                                   +      'latitude float(20, 14),'    
                                   +      'district varchar(32)'
                                   +      ')')
            conn.commit()
        #self._cursor.close()
        #conn.close()
        
    def insert_row(self, shop):
        try:
            self._cursor.execute('insert into ' + self._tb_name  + ' (id, name, branch_name, price, ' \
                                + 'star, taste, surroundings, service, category, longitude, latitude, district) ' \
                                #+ 'values (%d, %s, %s, %d, %s, %f, %f, %f)' \
                                + 'values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)' \
                                , shop.get_db_format())
            self._conn.commit()
            return True        
        except Exception as ex:
            if ex.errno == errorcode.ER_DUP_ENTRY:
                print(str(ex))  #重复添加，属正常逻辑
            else:
                print("fail to insert row to database. " + " error info: " + str(ex))
            return False

    def close(self):
        self._cursor.close()
        self._conn.close()

class DianpingCrawler(object):
    
    def __init__(self, db):
        self._restaurant = []
        self._db = db
    
    def do_crawler(self, region_idx):
        regionid = DianpingOption["regionids"][region_idx]
        self.get_restaurant_list_in_region(regionid)
    
    def get_restaurant_list_in_region(self, regionid):
        next_start = 0
        last_start = -1
        while next_start >= 0 and next_start > last_start:
            last_start = next_start
            next_start = self.parse_restaurant_list(next_start, regionid)
            print("collect restaurant num " + str(len(self._restaurant)))
            if next_start > DianpingOption["stop_threshold"]:
                print("restaurant number in one region go beyond the threshold.")
                break
        print("Crawling in region " + str(regionid) + " has finished.")
    
    def _get_list_url(self, start, regionid):
        sec_time = int(time.time())
        url = r"http://m.api.dianping.com/searchshop.json?" + "&regionid=" + str(regionid) + "&start=" + str(start) \
                 + r"&categoryid=" + str(DianpingOption['categoryid']) \
                 + r"&sortid=0&locatecityid=" + str(DianpingOption['locatecityid']) \
                 + r"&cityid=" + str(DianpingOption['cityid']) + r"&_=" + str(sec_time)
        return url

    def parse_restaurant_list(self, start, regionid):
        url = self._get_list_url(start, regionid)
        response = CrawlerCommon.get(url)
        try:
            json_dict = response.json()
            for list_node in json_dict["list"]:
                res = DianpingRestaurant(list_node["id"], list_node["name"], list_node["shopPower"], list_node["branchName"], \
                                         list_node["priceText"], list_node["categoryName"])
                
                if res.is_valid() and res.is_reasonable_data():
                    if self._db.insert_row(res): #insert ok, return true, otherwise return false
                        self._restaurant.append(res)
                    
                else:
                    print("skip restaurant " + list_node["name"] + " " + list_node["branchName"] + " id:" + str(list_node["id"]));
                    
            return json_dict["nextStartIndex"]
        except Exception as ex:
            self._dump_page('page_'+ str(start) + '_' + str(regionid) +'.json', response.text, response.encoding)
            print("Fail to get list of shop, start index " + str(start) + " region:" + str(regionid) + " ERR info:" + str(ex) \
                + " url: " + url)
            return -1
    
    def sorted_restaurants_by_price(self):
        self._restaurant.sort(key=lambda x:(x._price_num), reverse=True)
    
    def print_all_restaurant(self):
        for res in self._restaurant:
            print(res)
        print("restaurant num " + str(len(self._restaurant)));

    def _dump_page(self, path, str_content, encoding):
        with codecs.open(path, 'w', encoding)  as fp:
            fp.write(str_content)
        
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

    #参数是爬取的地区的下标 DianpingOption["regionids"]
    #启动不同进程来爬取不同地区，是为了避免被防爬虫机制禁用
    if len(sys.argv) > 1 :
        region_idx = int(sys.argv[1])
    else:
        region_idx = 0
       
    print("Region index " + str(region_idx) + " start crawling.\n")
    
    CrawlerCommon.session_init()
    db = DianpingDb('DianpingRes', 'ResTable', region_idx == 0)
    dc = DianpingCrawler(db);
    dc.do_crawler(region_idx)
    #dc.sorted_restaurants_by_price()
    db.close()
    #dc.print_all_restaurant()
    
    print("Region index " + str(region_idx) + " crawling ok\n")
    

if __name__ == "__main__":    
    main()
