#!/usr/bin/python3
# encoding: utf-8


import json
import traceback

import redis
import requests
import time

pool = redis.ConnectionPool(host='127.0.0.1', port=6379)
r = redis.Redis(connection_pool=pool)

WEIBO_PROXY_SET_KEY = 'weibo_proxy_set_key'
WEIBO_PROXY_418_SET_KEY = 'weibo_proxy_418_set_key'
WEIBO_ERROR_PROXY_SET_KEY = 'weibo_error_proxy_set_key'

target_money = "获取代理剩余金额接口URL"
target_ip = "获取代理IP接口URL"
targetUrl = "https://m.weibo.cn/api/container/getIndex?type=uid&value=1184728794&containerid=1076031184728794&page=3"
UserAgent = 'Mozilla/5.0 (iPhone; CPU iPhone OS 11_0 like Mac OS X) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/11.0 Mobile/15A372 Safari/604.1'


def request_proxy_ip():
    # 1. 获取余额
    resp_money = requests.get(url=target_money, timeout=5)
    money_json = json.loads(resp_money.text)
    print(money_json)
    price = money_json.get('data').get('balance')
    print(price)

    if float(price) > 20:
        # 2. 获取IP
        resp_ip = requests.get(url=target_ip, timeout=5)
        ip_json = json.loads(resp_ip.text)
        ips = ip_json.get('data')
        print(ips)
        for ip in ips:
            yield "http://" + ip['ip'] + ':' + str(ip['port'])
    else:
        print("余额不足")
        return None


def check_ip(ip):
    proxies = {
        "https": "http://" + ip
    }
    hearders = {
        "user-agent": UserAgent,
        "MWeibo-Pwa": '1',
        "Referer": 'https://m.weibo.cn/u/1184728794'
    }
    resp = requests.get(url=targetUrl, headers=hearders, proxies=proxies, timeout=5)
    return resp.status_code == 200


if __name__ == '__main__':

    try:
        while True:
            # 当代理IP小于10个的时候，需要重新补充IP源
            if r.scard(WEIBO_PROXY_SET_KEY) < 10:
                ips = request_proxy_ip()
                if ips is None:
                    break
                for ip in ips:
                    print(ip)
                    r.sadd(WEIBO_PROXY_SET_KEY, ip)

            # 维护418IP 池
            old_ip = r.srandmember(WEIBO_PROXY_418_SET_KEY, 1)
            if old_ip:
                # 监测ip是否有效
                try:
                    flag = check_ip(old_ip[0].decode())
                    if flag:
                        r.sadd(WEIBO_PROXY_SET_KEY, old_ip[0].decode())
                except Exception as  e:
                    r.srem(WEIBO_PROXY_418_SET_KEY, old_ip[0].decode())
                    r.sadd(WEIBO_ERROR_PROXY_SET_KEY, old_ip[0].decode())

            time.sleep(60)

    except Exception as  e:
        print('traceback.format_exc():\n%s' % traceback.format_exc())
