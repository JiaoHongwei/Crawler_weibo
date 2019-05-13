import redis

RESULT_KEY = 'weibo_result_list_key'

r = redis.Redis(host='127.0.0.1', port=6379)

if __name__ == '__main__':
    try:
        file_name = '/data/hw/result.txt'
        with open(file_name, 'a', encoding='utf-8') as fh:
            while r.llen(RESULT_KEY) > 0:
                value = r.lpop(RESULT_KEY).decode()
                # print(value)
                fh.write(value + '\n')
        fh.close()
    except Exception as e:
        # 如果发生错误则回滚
        print(str(e))
    finally:
        r.shutdown()
