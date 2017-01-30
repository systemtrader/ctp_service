ctp_service
===========

使用CTP接口实现数据获取、报撤单、账户查询等操作的demo。

使用前请在[http://simnow.sfit.com.cn/](http://simnow.sfit.com.cn/)注册投资者账号和密码，并修改`config.ini`。

参考文档：[CTP 客户端开发指南](http://www.sfit.com.cn/DocumentDown/api/CTPcdg_ch.pdf)。

环境要求
-------

需要安装下面的依赖：

- redis-server
- redis-py

参考下面的命令安装：

```
$ sudo apt install redis-server     # 安装redis-server
$ sudo service redis-server start   # 启动redis-server
$ sudo pip install redis            # 安装redis-py
```

注：目前仅支持Linux和Windows下的64位Python2.7.x。

项目说明
-------

```
ctp_service
|   .gitignore
│   README.MD
│   config.ini                      # 配置文件
|   ctp_collector.py                # 行情服务demo
|   ctp_trade.py                    # 交易服务demo
|   ctp_trade_test.py               # 交易服务demo测试
│
└───ctp                             # CTP封装
|   │   __init__.py                 # 接口定义
|   │   _MdApi.pyd                  # windows下的mdapi接口封装
|   |   _MdApi.so                   # linux下的mdapi接口封装
|   |   _TraderApi.pyd              # windows下的traderapi接口封装
|   |   _TraderApi.so               # linux下的traderapi接口封装
|   |   ApiStruct.py                # 数据结构体定义
|   |   libthostmduserapi.so        # linux下的mdapi依赖库
|   |   libthosttraderapi.so        # linux下的traderapi依赖库
|   |   thostmduserapi.dll          # windows下的mdapi依赖库
|   |   thosttraderapi.dll          # windows下的traderapi依赖库
|
└───data                            # 项目依赖的数据
│   │   calendar.txt                # 交易日历
|
└───tmp                             # CTP接收数据缓存目录
```

使用方法
-------

- 启动数据服务：

```
$ python ctp_collector.py config.ini day   # 启动日盘数据服务
$ python ctp_collector.py config.ini night # 启动夜盘数据服务
```

- 启动交易服务：

```
$ python ctp_trade.py config.ini
```
