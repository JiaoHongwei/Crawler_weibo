
## Python 3.6 抓取微博m站数据

本脚本截止20190307，实测了下目前微博m站（https://m.weibo.cn/）、wap站（https://weibo.cn/）、c站（https://weibo.com/）都是会封IP的，至于之前好多构建账号池来爬取wap站的博客试了试都失败了，而且字段解析都有问题，毕竟微博的数据格式也在更新。所以自己看了看python语法，写了两个脚本来抓取，现在不使用代理单机1天50万条数据左右，日常足以。

关于微博数据解析的自行百度，我只解析了微博时间和机型，其他的有需要自行添加就可以。


- 网址： https://m.weibo.cn/u/5463009082 新浪微博m站（智能手机网页端）</br> 
- api ： https://m.weibo.cn/api/container/getIndex?type=uid&value=5463009082&containerid=1076031000258404</br> 
- 抓取： 根据用户userId抓取历史发布的微博信息</br> 
- 方法： python请求api接口（非页面）</br> 
- 反扒： 疯狂抓取10~20s，IP会被禁封1~3分钟</br> 
- 备注： 无需登录+IP代理池</br> 
- 环境： Windosw或Linux、Python 3.6、Mysql、Redis</br> 


类型 | 效果
---|---
单机版（多线程）| 50万/天
单机版（多线程）+IP代理池| 150万/天
分布式（多线程）+IP代理池 | 千万~亿级


### 目录

- senior	# 分布式多线程爬取
- simple	# 单机版单线程爬取（可以自己改成多线程或者直接开多个窗口运行）
- venv	
- redis_consumer.py	   # redis 消费者 负责存储数据到数据库或者文件
- redis_userIds.py	   # redis 生产id 负责将用户id存储进redis 队列
- weibo.sql           # 表结构文件

### 数据库

* 可以选择直接入库，如果怕性能影响，也可以先存入文件或者存入redis队列，后续再慢慢入库
* Mysql 5.5
* weibo.sql中有建表语句
* 用户微博表 分表10张 用户表 1张

### 代理

* 本文提供了使用代理爬取的代码，如果对速度有要求，请购买https代理，使用redis稳定维护一个Htpps代理池。
* 注意微博是Https不能使用http代理，我测试了好多免费的http、https代理都不能用。
然后买了点发现还可以，但是效果不是很理想，Htpps代理延迟高，速度比不上Http。不过打发那被禁的3分钟绰绰有余。
* 实测不使用代理，一天在50万条数据左右


### Redis

* redis本来是没有用的，后面做分布式的时候才加入进来，单机的话也可以去掉。可以换成直接在文件读取，简单方便。


### 分布式爬取：

#### 原理：

利用redis的list实现一个生产者-消费者模型，分布式爬取<br/>

核心：Redis 列表(List)<br/>
环境：Python 3.6<br/>

#### 具体：

每台机器都去Redis中获取userIds队列中的用户id，进行爬取，爬取结果再装入redis result队列。最后有一台机器负责从result队列中消费爬取的数据，可选择入库或者存入文件中。这样可以在多台机器共同运行，拓展也比较方便，copy过去 start_senior.py文件，只要有环境就能直接运行。所以可以召集小伙伴的电脑每个人都打开帮你爬数据丫丫，哈哈，只占用点CPU和带宽资源吧，因为IP限制所以对性能影响不大。


### 步骤：

1. 整理userId，存入userIds.txt文档
2. 运行redis_userIds.py 将id信息存入redis队列
3. （选择使用代理）将代理IP存入redis
4. （选择存入数据库）创建数据库和表
5. 直接运行start_simple.py或者start_senior.py 开始抓取


### 注意：

* 微博m站现在已经有部分节点的数据结果返回集和url改了，现在是 page 分页，之后应该会是 senior_id 作用其实和page 一样，只不过 senior_id 是上一页返回的参数，即下一页从哪个ID开始。换一下URL参数就可以了
