import redis

RESULT_KEY = 'hw:weibo_results'
REDIS_KEY = 'hw:weibo_spider:userIds'

# redis 消费者

r = redis.Redis(host='127.0.0.1', port=6379)

if __name__ == '__main__':
    file_name = 'E:\\crawler\\result\\20190307_3.txt'
    with open(file_name, 'a', encoding='utf-8') as f:
        # 全部爬取完结束退出
        while r.llen(REDIS_KEY) > 0 or r.llen(RESULT_KEY) > 0:
            # 阻塞取出，没有会继续等待
            value = r.blpop(RESULT_KEY, 0)
            value1 = value[1].decode()
            print(value1)
            f.write(value1 + '\n')
    f.close()
