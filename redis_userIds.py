#!/usr/bin/python3

import redis as redis

REDIS_KEY = 'hw:weibo_spider:userIds'

r = redis.Redis(host='127.0.0.1', port=6379)

if __name__ == '__main__':
    try:
        path = 'E:\\crawler\\data\\userIds.txt'
        with open(path, mode='r', encoding='UTF-8') as  f:
            lines = f.readlines()
            for line in lines:
                userId = line.strip()
                r.lpush(REDIS_KEY, userId)
                print(userId)

    except Exception as e:
        print(str(e))
    finally:
        r.close()
