#!/bin/bash

district_num=14


# main.py的参数是爬取的地区的下标 DianpingOption["regionids"]
# 启动不同进程来爬取不同地区，是为了避免被防爬虫机制拦截

for((i=0;i<$district_num;i++));
    do
        python3 main.py $i
    done
