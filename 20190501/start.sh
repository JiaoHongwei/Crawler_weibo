#!/bin/bash
# 开始爬虫程序;

for((i=1;i<=5;i++));  
do   
python /data/hw/single_start.py 1>/dev/null 2>&1 &
echo "开启爬虫程序进程"+$i  
done 

sleep 5s
echo "已开启以下进程"
ps -ef|grep python 
