from redis_cli import r_operate
from zk_cli import zk_operate
from file_util import get_file_md5
import json

import logging


def get_latest_model(zk_url, node):
    def _child_node(zk_cli, node):
        children = zk_cli.get_children(node)
        children.sort()
        logging.debug("{}的子节点为：{}".format(node, children))
        latest_node = children[-1]
        logging.debug("最新的节点为:{}".format(latest_node))
        if (latest_node):
            node = node + '/' + latest_node
            return zk_cli.get(node)
        else:
            return None

    # 返回的是元组 ，需要使用第一个才是ZNode的data信息
    zk_info = zk_operate(zk_url=zk_url, node=node, func=lambda zk_cli, node: _child_node(zk_cli, node))[0]
    return json.loads(zk_info)


def download(local_file_path, zk_url, node):
    latest_model = get_latest_model(zk_url=zk_url, node=node)
    logging.debug("最新的模型-{}".format(latest_model))
    file = _download(local_file_path, latest_model)


def _download(local_file_path, model, loop=0):
    # 重试终止条件
    if loop >= 5:
        logging.error("下载重试第{}次，终止下载，请查看原始文件是否完整".format(loop))
        return

    def _load(redis_cli, block_count):
        with open(local_file_path, 'wb') as file:
            for i in range(block_count):
                block_content = redis_cli.hget("%s.%s" % (model['model_name'], model['deploy_time']), "block_" + str(i))
                file.write(block_content)
                # logging.debug("写入数据:{}".format(block_content))
            return file

    block_count = int(model['file_size']) // int(model['block_size']) + 1
    result = r_operate(func=lambda redis_cli, block_count=block_count: _load(redis_cli, block_count))

    logging.debug("下载的文件md5值为-{},原始文件的md5值为-{}".format(get_file_md5(local_file_path), model['md5']))
    if model['md5'] == get_file_md5(local_file_path):
        logging.info("{}模型下载成功".format(local_file_path))
    else:
        loop += 1
        logging.error("{}模型下载失败...重试第{}次".format(local_file_path, loop))
        _download(local_file_path, model, loop)
    return result


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    download(local_file_path="tmp", zk_url="172.25.47.80:2181", node="aaa")
