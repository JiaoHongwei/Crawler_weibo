#!/usr/bin/python3
# encoding: utf-8
import datetime
import json
import random
import re
import threading
import traceback
import urllib.request

import redis
import time

# 通用配置
WEIBO_UID_LIST_KEY = 'weibo_uid_list_key'
WEIBO_UID_SET_KEY = 'weibo_uid_set_key'

WEIBO_URL_LIST_KEY = 'weibo_url_list_key:'
WEIBO_RESULT_LIST_KEY = 'weibo_result_list_key'
WEIBO_PROXY_SET_KEY = 'weibo_proxy_set_key'
WEIBO_PROXY_418_SET_KEY = 'weibo_proxy_418_set_key'
WEIBO_ERROR_PROXY_SET_KEY = 'weibo_error_proxy_set_key'
pool = redis.ConnectionPool(host='127.0.0.1', port=6379)

r = redis.Redis(connection_pool=pool)

# 全局变量

# 是否使用代理
IF_PROXY = False
# 代理IP地址
PROXY_IP = ''
# 使用代理开始时间
START_TIME = int(round(time.time() * 1000))
# 不使用代理
null_proxy_handler = urllib.request.ProxyHandler({})


# 发送请求 使用代理
def use_proxy(url, user_id):
    global IF_PROXY, START_TIME, PROXY_IP
    while True:
        try:
            urllib.request.urlcleanup()
            req = urllib.request.Request(url)
            req.add_header('user-agent', getheaders())
            req.add_header('MWeibo-Pwa', 1)
            req.add_header('Referer', 'https://m.weibo.cn/u/' + str(user_id))

            # 如果使用https代理
            if IF_PROXY:
                # 如果代理使用超过1分钟，下次不再使用代理，优先使用本地ip
                end_time = int(round(time.time() * 1000))
                tt = end_time - START_TIME
                # print(str(tt) + 'ms')
                if tt > 60000:
                    IF_PROXY = False
                    print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                          + '【' + threading.current_thread().getName() + '】proxy used more than 60s, next try to use local proxy')

                if PROXY_IP == '':
                    tmp = get_proxy_ip()
                    if PROXY_IP == '':
                        PROXY_IP = tmp
                        print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                              + '【' + threading.current_thread().getName() + '】'
                              + 'get proxy success, ' + PROXY_IP)
                # 如果redis代理池没有代理IP了
                if PROXY_IP is None:
                    IF_PROXY = False
                    # 此时会使用本机IP进行访问，需要休眠30s,过去ip限制
                    print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                          + '【' + threading.current_thread().getName() + '】time sleep 30s,try to use local ip proxy')
                    time.sleep(30)
                    PROXY_IP = ''
                    opener = urllib.request.build_opener(null_proxy_handler, urllib.request.HTTPSHandler)
                else:
                    proxy_handler = urllib.request.ProxyHandler({'https': PROXY_IP})
                    opener = urllib.request.build_opener(proxy_handler, urllib.request.HTTPSHandler)
            else:
                opener = urllib.request.build_opener(null_proxy_handler, urllib.request.HTTPSHandler)
            urllib.request.install_opener(opener)
            with urllib.request.urlopen(req) as f:
                if f.status == 200:
                    data = f.read().decode('utf-8', 'ignore')
                    json_data = json.loads(data)
                    return json_data
        except urllib.error.HTTPError as e:
            # 如果使用代理出现异常，放入代理池
            IP = ''
            # 之前使用代理了，418
            if IF_PROXY:
                IP = PROXY_IP
                PROXY_IP = ''
                IF_PROXY = False
                # 将出现418的ip 取出 放在418队列，等待下次使用，有效避免重复尝试
                r.srem(WEIBO_PROXY_SET_KEY, IP)
                r.sadd(WEIBO_PROXY_418_SET_KEY, IP)

            else:
                # 没有使用代理，本地出现异常，换代理继续
                IF_PROXY = True
                START_TIME = int(round(time.time() * 1000))
            if e.code == 418:
                print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                      + '【' + threading.current_thread().getName() + '】【' + IP + '】ip http 418. ')
            if e.code == 413:
                print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                      + '【' + threading.current_thread().getName() + '】【' + IP + '】ip http 413. ')

        except Exception as e:
            # print('traceback.format_exc():\n%s' % traceback.format_exc())
            print(time.strftime('%Y-%m-%d %H:%M:%S',
                                time.localtime(
                                    time.time())) + '【' + threading.current_thread().getName()
                  + '】proxy ip connect error, remove ' + PROXY_IP)
            # 代理服务连接失败，失效了，从代理服务器列表移除

            if PROXY_IP != '':
                r.srem(WEIBO_PROXY_SET_KEY, PROXY_IP)
                r.sadd(WEIBO_ERROR_PROXY_SET_KEY, PROXY_IP)

            PROXY_IP = ''
            # 出现异常，换代理继续
            IF_PROXY = True
            START_TIME = int(round(time.time() * 1000))
            pass


# 返回一个随机的请求头 headers
def getheaders():
    headers = 'Mozilla/5.0 (iPhone; CPU iPhone OS 11_0 like Mac OS X) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/11.0 Mobile/15A372 Safari/604.1'
    return headers


# 格式化日期
def time_fix(time_string):
    now_time = datetime.datetime.now()
    if '刚刚' in time_string:
        # minutes = re.search(r'^(\d+)分钟', time_string).group(1)
        # created_at = now_time - datetime.timedelta(minutes=int(minutes))
        return now_time.strftime('%Y-%m-%d')

    if '分钟前' in time_string:
        # minutes = re.search(r'^(\d+)分钟', time_string).group(1)
        # created_at = now_time - datetime.timedelta(minutes=int(minutes))
        return now_time.strftime('%Y-%m-%d')

    if '小时前' in time_string:
        minutes = re.search(r'^(\d+)小时', time_string).group(1)
        created_at = now_time - datetime.timedelta(hours=int(minutes))

        return created_at.strftime('%Y-%m-%d')
    if '今天' in time_string:
        return now_time.strftime('%Y-%m-%d')

    if '昨天' in time_string:
        created_at = now_time - datetime.timedelta(days=1)
        return created_at.strftime('%Y-%m-%d')

    if '月' in time_string:  # 03月01日
        time_string = time_string.replace('月', '-').replace('日', '')
        time_string = str(now_time.year) + '-' + time_string
        return time_string

    if '-' in time_string and len(time_string) == 5:  # 03-01 格式
        return str(now_time.year) + '-' + time_string

    return time_string


# 随即返回一个 代理IP
def get_proxy_ip():
    global PROXY_IP
    try:
        ip = r.srandmember(WEIBO_PROXY_SET_KEY, 1)
        if ip:
            # decode() 对取出的值进行编码，有一个'b'代表的是bytes
            # 获取 redis ip list 第一个元素，不移除
            PROXY_IP = ip[0].decode()
            proxyMeta = str(PROXY_IP)
            return proxyMeta
        return None
    except Exception as  e:
        print('traceback.format_exc():\n%s' % traceback.format_exc())
        return None


# 保存到redis中
def save_weibo_info_to_redis_queue(results):
    for result in results:
        r.rpush(WEIBO_RESULT_LIST_KEY, result)


class GeneratorURL(threading.Thread):
    def __init__(self, name, user_id, container_id, if_proxy, proxy_ip, start_time):
        threading.Thread.__init__(self, name=name)
        self.user_id = user_id
        self.container_id = container_id
        self.if_proxy = if_proxy
        self.proxy_ip = proxy_ip
        self.start_time = start_time

    def run(self) -> None:
        print('thread is ' + threading.current_thread().getName() + ' start!')
        # 循环 从redis 中取出 userId
        self.start_time = int(round(time.time() * 1000))
        self.if_proxy = False
        self.proxy_ip = ''
        try:
            while r.llen(WEIBO_UID_LIST_KEY) > 0:
                # decode() 对取出的值进行编码，有一个'b'代表的是bytes
                id = r.lpop(WEIBO_UID_LIST_KEY)
                if id:
                    self.user_id = str(id.decode())
                    try:
                        self.container_id = "107603" + self.user_id
                        # 获取单个用户的所有微博信息,组装所有的url
                        results = self.get_weibo_first_page_info(self.user_id, self.container_id)
                        # 存入redis队列
                        save_weibo_info_to_redis_queue(results)


                    except AttributeError as e:
                        print('error ' + self.user_id)
                        continue
                    except Exception as e:
                        print('traceback.format_exc():\n%s' % traceback.format_exc())
                        continue
        except Exception as  e:
            # 防止redis异常 线程中断退出
            print('traceback.format_exc():\n%s' % traceback.format_exc())
            time.sleep(random.random() * 3)

    pass

    def get_weibo_first_page_info(self, user_id, container_id):
        url = 'https://m.weibo.cn/api/container/getIndex?type=uid&value=' + user_id + '&containerid=' + container_id
        try:
            # data = self.use_proxy(url, user_id)
            data = use_proxy(url, user_id)
            if data.get('ok') == 0:
                return []
            response = data.get('data')
            total = response['cardlistInfo']['total']
            # 先保存第一页的数据
            items = response.get('cards')
            print(time.strftime('%Y-%m-%d %H:%M:%S',
                                time.localtime(
                                    time.time())) + '【' + self.name + '】【' + PROXY_IP + '】start  ' + self.user_id + ' ,page 1')
            if total > 10:
                # 计算总页数
                page_num = int(int(total) / 10) + 1
                # 遍历总页数 生成URL 存入REDIS
                for page in range(2, page_num + 1):
                    page_url = 'https://m.weibo.cn/api/container/getIndex?' \
                               'type=uid&value=' + user_id + '&containerid=' + container_id + '&page=' + str(page)
                    # 组装key = WEIBO_URL_LIST_KEY + user_id，左边push
                    r.lpush(WEIBO_URL_LIST_KEY + user_id, page_url)
            else:
                # 只有1页，所以直接删除 uid
                r.srem(WEIBO_UID_SET_KEY, user_id)

            # 保存数据到redis
            for item in items:
                item = item.get('mblog')
                if item:
                    # 2015年之前的数据直接放弃，没有多少智能手机设备
                    date = time_fix(item.get('created_at'))
                    if int(date[0:4]) < 2015:
                        break
                    results = user_id + '\t' + date + '\t' + item.get('source') + '\t' + str(1)
                    yield results
        except Exception as e:
            print('traceback.format_exc():\n%s' % traceback.format_exc())
            return []
            # 热门微博网页数
        pass

        # 发送请求

    def use_proxy(self, url, user_id):
        while True:
            try:
                urllib.request.urlcleanup()
                req = urllib.request.Request(url)
                req.add_header('user-agent', getheaders())
                req.add_header('MWeibo-Pwa', 1)
                req.add_header('Referer', 'https://m.weibo.cn/u/' + str(user_id))
                # print(url)

                # 如果使用https代理
                if self.if_proxy:
                    # 如果代理使用超过1分钟，下次不再使用代理，优先使用本地ip
                    end_time = int(round(time.time() * 1000))
                    tt = end_time - self.start_time
                    # print(str(tt) + 'ms')
                    if tt > 60000:
                        self.if_proxy = False
                        print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                              + '【' + self.name + '】'
                              + '【' + self.proxy_ip + '】 proxy used more than 60s, next try to use local proxy')

                    if self.proxy_ip == '':
                        self.proxy_ip = get_proxy_ip()
                    # 如果redis代理池没有代理IP了
                    if self.proxy_ip is None:
                        self.if_proxy = False
                        # 此时会使用本机IP进行访问，需要休眠30s,过去ip限制
                        print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                              + '【' + self.name + '】time sleep 30s,try to use local ip proxy')
                        time.sleep(30)
                        self.proxy_ip = ''
                    else:
                        proxy = urllib.request.ProxyHandler({'https': self.proxy_ip})
                        opener = urllib.request.build_opener(proxy, urllib.request.HTTPSHandler)
                        urllib.request.install_opener(opener)
                with urllib.request.urlopen(req) as f:
                    if f.status == 200:
                        data = f.read().decode('utf-8', 'ignore')
                        json_data = json.loads(data)
                        return json_data
            except urllib.error.HTTPError as e:
                # 如果使用代理出现异常，放入代理池
                IP = ''
                # 之前使用代理了，418
                if self.if_proxy:
                    IP = self.proxy_ip
                    self.proxy_ip = ''
                    self.if_proxy = False
                    # 将出现418的ip 取出 放在418队列，等待下次使用，有效避免重复尝试
                    r.srem(WEIBO_PROXY_SET_KEY, IP)
                    r.sadd(WEIBO_PROXY_418_SET_KEY, IP)

                else:
                    # 没有使用代理，本地出现异常，换代理继续
                    self.if_proxy = True
                    self.start_time = int(round(time.time() * 1000))
                if e.code == 418:
                    print(time.strftime('%Y-%m-%d %H:%M:%S',
                                        time.localtime(
                                            time.time())) + '【' + self.name + '】【' + self.proxy_ip + '】ip http 418. ')
                if e.code == 413:
                    print(time.strftime('%Y-%m-%d %H:%M:%S',
                                        time.localtime(
                                            time.time())) + '【' + self.name + '】【' + self.proxy_ip + '】ip http 413. ')

            except Exception as e:
                # print('traceback.format_exc():\n%s' % traceback.format_exc())
                print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                      + '【' + self.name + '】  proxy ip connect error, remove ' + self.proxy_ip)
                # 代理服务连接失败，失效了，从代理服务器列表移除

                if self.proxy_ip != '':
                    r.srem(WEIBO_PROXY_SET_KEY, self.proxy_ip)
                    r.sadd(WEIBO_ERROR_PROXY_SET_KEY, self.proxy_ip)

                self.proxy_ip = ''
                # 出现异常，换代理继续
                self.if_proxy = True
                self.start_time = int(round(time.time() * 1000))
        pass


class GeneratorResults(threading.Thread):
    def __init__(self, name, user_id, container_id, if_proxy, proxy_ip, start_time):
        threading.Thread.__init__(self, name=name)
        self.user_id = user_id
        self.container_id = container_id
        self.if_proxy = if_proxy
        self.proxy_ip = proxy_ip
        self.start_time = start_time

    def run(self) -> None:
        print('thread is ' + threading.current_thread().getName() + ' start!')
        # 循环 从redis 中取出 userId
        self.start_time = int(round(time.time() * 1000))
        self.if_proxy = False
        self.proxy_ip = ''
        try:
            while r.scard(WEIBO_UID_SET_KEY) > 0:
                # decode() 对取出的值进行编码，有一个'b'代表的是bytes
                # 随机返回sorted中的一个uid，并不移除
                id = r.srandmember(WEIBO_UID_SET_KEY)
                if id:

                    self.user_id = str(id.decode())
                    # 查询当前uid 是否还有url
                    key = WEIBO_URL_LIST_KEY + self.user_id
                    if r.llen(key) > 0:
                        # 获取page最小的url，开始抓取
                        url = r.rpop(key)
                        if url:
                            url = url.decode()
                            try:
                                # 获取当前page
                                p1 = r'&page=(.*)'  # 想匹配到page后面的值
                                pattern = re.compile(p1)
                                page = pattern.findall(url)[0]

                                # 获取单个url的所有信息
                                results = self.get_weibo_single_page_info(self.user_id, key, url, page)
                                # 存入redis队列
                                save_weibo_info_to_redis_queue(results)

                            except AttributeError as e:
                                print('error ' + self.user_id)
                                continue
                            except Exception as e:
                                print('traceback.format_exc():\n%s' % traceback.format_exc())
                                continue
                        else:
                            r.srem(WEIBO_UID_SET_KEY, self.user_id)
                    else:
                        # 1.删掉uid,即当前uid已经抓取完了
                        r.srem(WEIBO_UID_SET_KEY, self.user_id)
            print('thread is ' + threading.current_thread().getName() + ' end!')
        except Exception as  e:
            # 防止redis异常 线程中断退出
            print('traceback.format_exc():\n%s' % traceback.format_exc())
            time.sleep(random.random() * 3)

    def get_weibo_single_page_info(self, user_id, key, url, page):
        try:
            # page_data = self.use_proxy(url, user_id)
            page_data = use_proxy(url, user_id)
            if page_data.get('ok') == 0:
                return []
            print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(
                time.time())) + '【' + self.name + '】【' + PROXY_IP + '】start  ' + user_id + ' ,page ' + str(page))

            items = page_data.get('data').get('cards')

            for item in items:
                item = item.get('mblog')
                if item:
                    # 2015年之前的数据直接放弃
                    date = time_fix(item.get('created_at'))
                    if int(date[0:4]) < 2015:
                        # 1.删掉uid,即当前uid已经抓取完了
                        r.srem(WEIBO_UID_SET_KEY, user_id)
                        # 2.删掉url,剩下的url不需要再继续抓取
                        r.delete(key)
                        break

                    results = user_id + '\t' + date + '\t' + item.get('source') + '\t' + str(page)
                    # print(results)
                    yield results


        except Exception as e:
            # print('traceback.format_exc():\n%s' % traceback.format_exc())
            return []

    def use_proxy(self, url, user_id):
        while True:
            try:
                urllib.request.urlcleanup()
                req = urllib.request.Request(url)
                req.add_header('user-agent', getheaders())
                req.add_header('MWeibo-Pwa', 1)
                req.add_header('Referer', 'https://m.weibo.cn/u/' + str(user_id))
                # print(url)

                # 如果使用https代理
                if self.if_proxy:
                    # 如果代理使用超过1分钟，下次不再使用代理，优先使用本地ip
                    end_time = int(round(time.time() * 1000))
                    tt = end_time - self.start_time
                    # print(str(tt) + 'ms')
                    if tt > 60000:
                        self.if_proxy = False
                        print(time.strftime('%Y-%m-%d %H:%M:%S',
                                            time.localtime(
                                                time.time())) + '【' + self.name + '】  proxy used more than 60s, next try to use local proxy')

                    if self.proxy_ip == '':
                        self.proxy_ip = get_proxy_ip()
                    # 如果redis代理池没有代理IP了
                    if self.proxy_ip is None:
                        self.if_proxy = False
                        # 此时会使用本机IP进行访问，需要休眠30s,过去ip限制
                        print(time.strftime('%Y-%m-%d %H:%M:%S',
                                            time.localtime(
                                                time.time())) + '【' + self.name + '】  time sleep 30s,try to use local ip proxy')
                        time.sleep(30)
                        self.proxy_ip = ''
                    else:
                        proxy = urllib.request.ProxyHandler({'https': self.proxy_ip})
                        opener = urllib.request.build_opener(proxy, urllib.request.HTTPSHandler)
                        urllib.request.install_opener(opener)
                with urllib.request.urlopen(req) as f:
                    if f.status == 200:
                        data = f.read().decode('utf-8', 'ignore')
                        json_data = json.loads(data)
                        return json_data
            except urllib.error.HTTPError as e:
                # 如果使用代理出现异常，放入代理池
                IP = ''
                # 之前使用代理了，418
                if self.if_proxy:
                    IP = self.proxy_ip
                    self.proxy_ip = ''
                    self.if_proxy = False
                    # 将出现418的ip 取出 放在418队列，等待下次使用，有效避免重复尝试
                    r.srem(WEIBO_PROXY_SET_KEY, IP)
                    r.sadd(WEIBO_PROXY_418_SET_KEY, IP)

                else:
                    # 没有使用代理，本地出现异常，换代理继续
                    self.if_proxy = True
                    self.start_time = int(round(time.time() * 1000))
                if e.code == 418:
                    print(time.strftime('%Y-%m-%d %H:%M:%S',
                                        time.localtime(time.time())) + '【' + self.name + '】  ip http 418. ' + IP)
                if e.code == 413:
                    print(time.strftime('%Y-%m-%d %H:%M:%S',
                                        time.localtime(time.time())) + '【' + self.name + '】  ip http 413. ' + IP)

            except Exception as e:
                # print('traceback.format_exc():\n%s' % traceback.format_exc())
                print(time.strftime('%Y-%m-%d %H:%M:%S',
                                    time.localtime(
                                        time.time())) + '【' + self.name + '】  proxy ip connect error, remove ' + self.proxy_ip)
                # 代理服务连接失败，失效了，从代理服务器列表移除

                if self.proxy_ip != '':
                    r.srem(WEIBO_PROXY_SET_KEY, self.proxy_ip)
                    r.sadd(WEIBO_ERROR_PROXY_SET_KEY, self.proxy_ip)

                self.proxy_ip = ''
                # 出现异常，换代理继续
                self.if_proxy = True
                self.start_time = int(round(time.time() * 1000))
        pass


if __name__ == '__main__':
    print('step1. 开始抓取每个UID第一页，生成所有的URL')
    threads = []
    for i in range(0, 5):
        thread = GeneratorURL(name=str(i), user_id=None, container_id=None, if_proxy=None, proxy_ip=None,
                              start_time=None)
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join()

    print('step2. 开始抓取所生成的URL')
    threads = []
    for i in range(0, 5):
        thread = GeneratorResults(name=str(i), user_id=None, container_id=None, if_proxy=None, proxy_ip=None,
                                  start_time=None)
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join()

    print('Main Thread finish')
    pool.disconnect()
