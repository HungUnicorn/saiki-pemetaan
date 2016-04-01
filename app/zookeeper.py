from kazoo.client import KazooClient
from kazoo.handlers.threading import KazooTimeoutError
import os
import logging


def init_zk(namespace=''):
    try:
        zk = KazooClient(hosts=os.getenv('ZOOKEEPER_CONN_STRING') + namespace,
                         read_only=False)
        zk.start()
        if type(zk) is not KazooClient:
            logging.error("can't connect to Zookeeper ...")
            exit(1)
        return zk
    except KazooTimeoutError:
        logging.error("can't connect to Zookeeper ...")
        exit(1)


def get_namespace_kafka():
    return os.getenv('ZOOKEEPER_NAMESPACE_KAFKA')


def get_namespace_saiki():
    return os.getenv('ZOOKEEPER_NAMESPACE_SAIKI')


def get_namespace_pemetaan():
    return os.getenv('ZOOKEEPER_NAMESPACE_PEMETAAN')
