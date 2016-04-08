from kazoo.client import KazooClient, NodeExistsError
from kazoo.handlers.threading import KazooTimeoutError
import os
import logging
import json


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


def change_topic_config(zk, topic, config_dict):
    """
    Update the config for an existing topic and create a change notification
    so the change will propagate to other brokers
    """
    update_config(zk, topic, config_dict)
    notify_change(zk, topic, config_dict)
    logging.info("created/updated topic config: topic: " + topic +
                 " , config : " + str(config_dict))


def update_config(zk, topic, config_dict):
    try:
        zk.create('/config/topics/' + topic,
                  json.dumps(config_dict, separators=(',', ':'))
                  .encode('utf-8'), makepath=True)
    except NodeExistsError:
        zk.set('/config/topics/' + topic,
               json.dumps(config_dict, separators=(',', ':')).encode('utf-8'))


def notify_change(zk, topic, config_dict):
    node = '/config/changes/config_change_1'
    content = {'version': 1,
               'entity_type': 'topics',
               'entity_name': topic}
    try:
        zk.create(node, json.dumps(content, separators=(',', ':'))
                  .encode('utf-8'), makepath=True)
    except NodeExistsError:
        zk.set('/config/changes/config_change_',
               json.dumps(content, separators=(',', ':'))
               .encode('utf-8'))
