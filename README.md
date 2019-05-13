
## Python 3.6 抓取微博m站数据

### 2019.05.01 更新内容

1.  containerid 可以通过 `"107603" + user_id` 组装得到，无需请求个人信息获取；
2.  优化多线程抓取，修复之前因`urllib.request`全局定义，导致代理无法正常切回本地IP；
3.  优化分布式抓取策略，由每台机器顺序获取用户ID 再拼装URL抓取策略，修改为每台机器顺序获取URL进行抓取。防止由于某个微博账号微博动态过多导致负责本ID的机器长时间运行，而其他机器就要等待，浪费时间和资源。
4.  加入IP代理池维护脚本，可以定时维护代理池中的有效代理IP，及时剔除无效IP。
5.  加入Redis定时消费脚本，解决因抓取结果过大导致redis性能下降。
6.  增加Redis连接池`ConnectionPool` ，解决因Redis链接端口数过多导致Redis拒绝服务错误。
7.  调整Redis数据存储结构，采用list+set结合，存储UID+URL
8.  单机https代理消费约100元/天，单机5个进程+代理每天能达到1000万条数据

### 本脚本截止20190501

- 网址： https://m.weibo.cn/u/5463009082 新浪微博m站（智能手机网页端）</br> 
- api ： https://m.weibo.cn/api/container/getIndex?type=uid&value=5463009082&containerid=1076035463009082</br> 
- 抓取： 根据用户userId抓取历史发布的微博信息</br> 
- 方法： python请求api接口（非页面）</br> 
- 反扒： 疯狂抓取10-20s，IP会被禁封1-3分钟 或者 抓取2分钟禁封10分钟</br> 
- 备注： 无需登录+IP代理池</br> 
- 环境： Windosw或Linux、Python 3.6、Mysql、Redis</br> 


类型 | 效果 | 代理花费
---|---|---
单机版（多线程+多进程）| 150万/天 | 0
单机版（多线程+多进程）+IP代理池 | 1000万/天 | 100元/天
分布式（多线程+多进程）+IP代理池 | 千万~亿级 | 100*机器数量/天

### 抓取流程

1.  启动`save_uid.py`脚本将准备好的微博用户ID，存入Redis中，保存两份；
    *   `r.lpush(WEIBO_UID_LIST_KEY, uid)`  用于第一次遍历UID请求每个UID的总页数，然后生成每个UID的所有URL。
    *   `r.sadd(WEIBO_UID_SET_KEY, uid)`    用于第二次请求，随机获取一个未抓取完的UID，去Redis中取当前UID剩余的URL进行抓取，全部抓取完毕则移除此UID。

2.  启动`proxy_pool.py`脚本，初始化IP代理池（如果不使用代理，此步骤可略过）
    * `target_money` 用来获取你所购买的IP代理剩余金额的URL连接
    * `target_ip`   请求代理IP获取有效的代理IP
    * `targetUrl`   用来校验当前IP是否有效
    * `WEIBO_PROXY_SET_KEY` 存储有效的代理IP
    * `WEIBO_PROXY_418_SET_KEY` 存储已经出现418错误的IP，会循环利用，直到代理失效
    * `WEIBO_ERROR_PROXY_SET_KEY` 存储已经使用过的IP（失效IP），用于后期校验

3. 启动`start_crawler.py`脚本，开启抓取任务
    *   首先会第一次请求遍历UID，生成所有的URL，然后所有的线程获取URL进行抓取
    *   先会尝试使用本地IP进行抓取，本地IP出现418之后，尝试去IP代理池获取可使用的IP，继续抓取
    *   使用代理IP抓取超过60s，停止代理抓取改为使用本地IP，循环进行
    *   代理IP出现418，则先去尝试使用本地IP，如果本地418再继续更换代理
    *   如果代理池没有可用IP，则休息30s，继续使用本地IP
    
4.  添加Linux定时脚本，定时处理Redis队列中的抓取结果。

### 关于代理IP

- 抓取微博的https连接，只能使用https代理，可选择市面上其他类型的代理商；
- 免费的代理ip也有尝试，不过效果不是很好

### 部署问题

- 需要一台主服务器（或者本机Windows电脑）来初始化运行 `save_uid.py` 和 `proxy_pool.py`脚本
- 在Redis所在的服务器进行`redis_consumer.py`脚本的运行部署
- `start_crawler.py` 可以集群部署到多台服务器，但要保证都能网络连通到Redis服务器
- 添加定时脚本`crontab -e` 内容 如下
    ```
    0 */1 * * * nohup python /data/hw/redis_consume.py > /dev/null 2>&1 &
    ```
- `service cron restart` 重启cron定时服务
- 创建start.sh 开启多进程抓取
    ```bash
    #!/bin/bash
    # 开始爬虫程序;
    
    for((i=1;i<=5;i++));
    do
    nohup python /data/hw/start_crawler.py 1>/dev/null 2>&1 &
    echo "开启爬虫程序进程"+$i  
    done
    
    sleep 5s
    echo "已开启以下进程"
    ps -ef|grep python
    
    ```

### 20190501目录

* proxy_pool.py     #维护IP代理池
* redis_consume.py  #redis定时消费
* save_uid.py       #初始化uid
* start.sh          #一键启动脚本
* start_crawler.py  #开启抓取程序，可以集群部署

## 旧版本README

[README_20190307](README_20190307.md)
