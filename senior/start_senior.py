#!/usr/bin/python3
# encoding: utf-8

import datetime
import json
import threading
import time
import traceback
import urllib.request

import redis

r = redis.Redis(host='127.0.0.1', port=6379)
REDIS_KEY = 'hw:weibo_spider:userIds'
PROXY_KEY = 'hw:proxies'
RESULT_KEY = 'hw:weibo_results'
headers = 'Mozilla/5.0 (iPhone; CPU iPhone OS 11_0 like Mac OS X) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/11.0 Mobile/15A372 Safari/604.1'
PROXY_IP = ''
num = 0
start_time = time.time()
mutex = threading.Lock()


# 格式化时间

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


# 发送代理请求
def use_proxy(url, userId):
    global PROXY_IP, num, start_time
    global key
    while True:
        try:
            urllib.request.urlcleanup()
            req = urllib.request.Request(url)
            req.add_header('User-Agent', headers)
            req.add_header('MWeibo-Pwa', 1)
            req.add_header('Referer', 'https://m.weibo.cn/u/' + str(userId))
            # 每次从队列的右边取出最新的代理IP
            if PROXY_IP != '':
                if r.get(key) is not None:
                    print("当前使用代理IP:" + PROXY_IP)
                    proxy = urllib.request.ProxyHandler({'https': PROXY_IP})
                    opener = urllib.request.build_opener(proxy, urllib.request.HTTPSHandler)
                    urllib.request.install_opener(opener)
                elif key is not None:
                    # 已经使用代理超过1分钟，切换回正常ip
                    print('已经使用代理超过1分钟，切换回正常ip')
                    r.lpush(PROXY_KEY, PROXY_IP)
            # 全局代理IP为空 不使用代理
            # print(url)
            with urllib.request.urlopen(req) as f:
                if f.status == 200:
                    data = f.read().decode('utf-8', 'ignore')
                    if mutex.acquire():
                        num += 1
                        mutex.release()
                    return data
        except urllib.error.HTTPError as e:
            print('ip http 418. ' + PROXY_IP + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
            print('共耗时' + str(time.time() - start_time) + ', 请求 ' + str(num) + ' 次')

            if e.code == 418:
                if PROXY_IP != '':
                    # 如果之前使用了代理，需要将之前的代理放置队列的最左边
                    r.lpush(PROXY_KEY, PROXY_IP)
                    # 代理失效之后尝试使用本地IP，即不使用代理
                    print('代理ip http 418，尝试使用本地ip访问...')
                    PROXY_IP = ''
                else:
                    # 本地IP失效，使用代理
                    print('本地ip http 418，切换代理中... ' + PROXY_IP)
                    try:
                        PROXY_IP = r.rpop(PROXY_KEY).decode()
                        # https 代理速度慢，所以能不使用就尽量避免，设置代理有效期1分钟，1分钟后自动切回原ip
                        key = "PROXY_IP." + PROXY_IP
                        r.set(key, time.time())
                        r.expire(key, 60)

                    except Exception as e:
                        # print('traceback.format_exc():\n%s' % traceback.format_exc())
                        print('redis 获取 代理ip失败，暂停60s尝试使用本地ip访问...')
                        time.sleep(60)
                        PROXY_IP = ''
                        start_time = time.time()
                        if mutex.acquire():
                            num = 0
                            mutex.release()
        except Exception as e:
            # print('traceback.format_exc():\n%s' % traceback.format_exc())
            r.delete(key)
            PROXY_IP = ''
    pass


# 计算表名
def find_user_table(userId):
    # hash 取模
    table_id = str(hash(userId) % 10)
    return 'data_' + table_id
    pass

# 获取微博主页的containerid，爬取微博内容时需要此id
# 获取微博大V账号的用户基本信息，如：微博昵称、微博地址、微博头像、关注人数、粉丝数、性别、等级等
# 这里我只需要获取containerid就可以了，其他信息如果有需要自行获取
def get_userInfo(userId):
    url = 'https://m.weibo.cn/api/container/getIndex?type=uid&value=' + userId
    data = use_proxy(url, userId)
    content = json.loads(data).get('data')
    for data in content.get('tabsInfo').get('tabs'):
        if (data.get('tab_type') == 'weibo'):
            containerid = data.get('containerid')
    return containerid


# 发送请求解析数据
def get_weibo_info(userId, containerid):
    table = find_user_table(userId)
    weibo_url = 'https://m.weibo.cn/api/container/getIndex?type=uid&value=' + userId + '&containerid=' + containerid
    try:
        data = use_proxy(weibo_url, userId)
        response = json.loads(data).get('data')
        total = response['cardlistInfo']['total']
        # 先保存第一页的数据
        items = response.get('cards')
        for item in items:
            item = item.get('mblog')
            if item:
                # 2014年之前的数据直接放弃，没有多少智能手机设备
                date = time_fix(item.get('created_at'))
                if int(date[0:4]) < 2014:
                    break
                # sql = "INSERT INTO " + table + " (user_id, date, os, page, create_time) value ('%s','%s','%s','%s','%s') " % (
                #     userId, date, item.get('source'), str(page),
                #     time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
                # yield sql
                results = userId + '\t' + date + '\t' + item.get('source') + '\t' + str(1) + '\t' + time.strftime(
                    '%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                yield results
    except Exception as e:
        # print('traceback.format_exc():\n%s' % traceback.format_exc())
        return []
        # 热门微博网页数
    break_flag = False
    # 大于10 才有第二页
    if total > 10:
        page_num = int(int(total) / 10) + 1
        for page in range(2, page_num + 1):
            page_url = 'https://m.weibo.cn/api/container/getIndex?' \
                       'type=uid&value=' + userId + '&containerid=' + containerid + '&page=' + str(page)
            try:
                page_data = use_proxy(page_url, userId)
                items = json.loads(page_data).get('data').get('cards')
            except Exception as e:
                continue
            for item in items:
                item = item.get('mblog')
                if item:
                    # 2014年之前的数据直接放弃
                    date = time_fix(item.get('created_at'))
                    if int(date[0:4]) < 2014:
                        break_flag = True
                        break
                    # sql = "INSERT INTO " + table + " (user_id, date, os, page, create_time) value ('%s','%s','%s','%s','%s') " % (
                    #     userId, date, item.get('source'), str(page),
                    #     time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
                    # yield sql
                    results = userId + '\t' + date + '\t' + item.get('source') + '\t' + str(
                        page) + '\t' + time.strftime(
                        '%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                    # print(results)
                    yield results
            if break_flag:
                break
    pass

# 保存数据到数据库
# def save_weibo_info(results):
#     # 使用cursor()方法获取操作游标
#     cursor = db.cursor()
#     # 执行sql语句
#     num = 0
#     try:
#         for sql in results:
#             num = num + 1
#             cursor.execute(sql)
#             # 提交到数据库执行 1万条提交1次
#             if num == 10000:
#                 # db.commit()
#                 num = 0
#         # 把不到10000 的直接提交
#         # db.commit()
#     except Exception as e:
#         # 如果发生错误则回滚
#         db.rollback()
#         print(str(e))
#     finally:
#         # 关闭游标
#         cursor.close()
#     # 关闭数据库连接
#     # db.close()
#     pass

# 更新用户状态
# def update_user_status(userId):
#     # 使用cursor()方法获取操作游标
#     cursor = db.cursor()
#     # 执行sql语句
#     sql = "UPDATE user SET status = 1 WHERE user_id = '%s'" % userId
#     try:
#         cursor.execute(sql)
#         db.commit()
#         print(sql)
#     except Exception as e:
#         # 如果发生错误则回滚
#         db.rollback()
#         print(str(e))
#     finally:
#         # 关闭游标
#         cursor.close()
#     # 关闭数据库连接
#     # db.close()
#     pass

# 保存到文件
def save_weibo_info_to_txt(results):
    file_name = 'E:\\crawler\\result\\20190305.txt'
    with open(file_name, 'a', encoding='utf-8') as fh:
        for result in results:
            fh.write(result + '\n')
    pass

# 保存到redis中
def save_weibo_info_to_redis_queue(results):
    for result in results:
        r.rpush(RESULT_KEY, result)


class CrawlerThread(threading.Thread):
    def run(self) -> None:
        print('thread is ' + threading.current_thread().getName() + ' start!')
        # time.sleep(5)
        # 循环 从redis 中取出 userId
        while r.llen(REDIS_KEY) > 0:
            # decode() 对取出的值进行编码，有一个'b'代表的是bytes
            userId = r.lpop(REDIS_KEY).decode()
            print(userId)
            try:
                # 获取用户的containerid
                containerid = get_userInfo(userId)
                # 获取单个用户的所有微博
                results = get_weibo_info(userId, containerid)
                # 保存用户微博信息到数据库
                # save_weibo_info(results)
                # save_weibo_info_to_txt(results)
                save_weibo_info_to_redis_queue(results)
                # 更新用户状态
                # update_user_status(userId)
            except AttributeError as e:
                print('用户不存在:' + userId)
                continue
            except Exception as e:
                # print('traceback.format_exc():\n%s' % traceback.format_exc())
                print("代理失效")
                continue


if __name__ == '__main__':
    threads = []
    for i in range(0, 5):
        thread = CrawlerThread(name=str(i))
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join()
    print('Main Thread finish')
