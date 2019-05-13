#!/usr/bin/python3
# encoding: utf-8


import redis as redis

# 1. 存入需要抓取的微博UID 格式 redis list （REDIS_KEY，UID）

# 通用配置
WEIBO_UID_LIST_KEY = 'weibo_uid_list_key'
WEIBO_UID_SET_KEY = 'weibo_uid_set_key'

pool = redis.ConnectionPool(host='127.0.0.1', port=6379)
r = redis.Redis(connection_pool=pool, decode_responses=True)

path = 'E:\\crawler\\data\\weibo_20190507.txt'

try:
    with open(path, mode='r', encoding='UTF-8') as  f:
        lines = f.readlines()
        for line in lines:
            uid = line.strip()
            r.lpush(WEIBO_UID_LIST_KEY, uid)
            r.sadd(WEIBO_UID_SET_KEY, uid)
            print(uid)
except Exception as e:
    # 如果发生错误则回滚
    print(str(e))
finally:
    pool.disconnect()
