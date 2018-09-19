"""
统一管理redis客户端的连接、关闭；连接时的操作由调用者实现func方法
"""
import redis
import os
import configparser

config = configparser.ConfigParser()
path = os.path.split(os.path.realpath(__file__))[0] + '/redis_cfg.ini'
config.read(path)

redis_host = config.get("redis", "host")
port = config.get("redis", "port")
redis_app_name = config.get("redis", "app_name")

pool = redis.ConnectionPool(host=redis_host, port=port, password=redis_app_name)


def r_operate(func):
    redis_cli = None
    try:
        redis_cli = redis.Redis(connection_pool=pool)
        result = func(redis_cli)
    finally:
        # 自动回收连接
        pass
    return result
