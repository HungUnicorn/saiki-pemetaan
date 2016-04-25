# coding=utf-8
"""Controller File."""
import json
import logging

import os
import redis
from kazoo.client import NoNodeError
from zookeeper import init_zk, get_namespace_pemetaan

r = redis.StrictRedis(host=os.getenv('REDIS_HOST'), port=6379, db=0)

namespace_pemetaan = get_namespace_pemetaan()


def get_html_tooltip(t_dict):
    """Docstring."""
    return_string = '<table>'
    for partition_key, value in t_dict["partitions"].items():
        return_string += '<tr><td>'
        return_string += partition_key + ' : ' + str(
            t_dict["partitions"][partition_key][
                'broker']) + '</td>'
        return_string += '<td>Leader: ' + \
                         str(t_dict["partitions"][partition_key]
                             ["state"]["leader"]) \
                         + '</td>'
        return_string += '<td>ISR: ' + str(t_dict["partitions"][partition_key]
                                           ["state"]["isr"]) \
                         + '</td><tr>'
    return_string += '</table>'
    return return_string


def get_settings():
    """Get Pemetaan Settings from ZK."""
    zk = init_zk(namespace_pemetaan)

    try:
        data, stat = zk.get('/settings')
        return json.loads(data.decode('utf-8'))
    except NoNodeError:
        return []


def update_settings(key_to_update, value_to_update):
    """Set Pemetaan Settings in ZK."""
    logging.info("setting new pemetaan settings ...")
    settings = get_settings()
    print(settings)
    print(key_to_update)
    print(value_to_update)
    settings[key_to_update] = value_to_update
    print(settings)
    zk = init_zk(namespace_pemetaan)
    zk.set('/settings', json.dumps(settings).encode('utf-8'))
