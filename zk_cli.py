"""
统一管理zk客户端的连接、关闭；连接时的操作由调用者实现func方法
"""
from kazoo.client import KazooClient


def zk_operate(zk_url, node, func):
    zk = KazooClient(hosts=zk_url)
    try:
        zk.start()
        base_path = "/models"
        node = "%s/%s" % (base_path, node)
        result = func(zk, node)
    finally:
        zk.stop()
        zk.close()
    return result
