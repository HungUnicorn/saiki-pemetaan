import datetime
import json

import jmx
import os
import redis
from kazoo.client import NoNodeError
from zookeeper import init_zk, get_namespace_kafka

r = redis.StrictRedis(host=os.getenv('REDIS_HOST'), port=6379, db=0)
namespace_kafka = get_namespace_kafka()


def get_jmx_metrics_for_topic(topic, brokers):
    """Docstring."""
    if 'error' in brokers[0]:
        return [brokers[0]]
    return_dict = {}
    for broker in brokers:
        return_dict["MessagesPerSecOneMinuteRate"] = format(
            float(jmx.get_metric_per_broker(
                broker["host"],
                jmx.key_messages_in_per_topic + topic,
                'OneMinuteRate')), '.2f')
        return_dict["BytesPerSecOneMinuteRate"] = format(
            float(jmx.get_metric_per_broker(
                broker["host"],
                jmx.key_bytes_in_per_topic + topic,
                'OneMinuteRate')), '.2f')
        if (return_dict["MessagesPerSecOneMinuteRate"] != '-1.00' and
                return_dict["BytesPerSecOneMinuteRate"] != '-1.00'):
            break
    return return_dict


def get_raw_brokers():
    """Docstring."""
    pass


def get_brokers():
    """Docstring."""
    zk = init_zk(namespace_kafka)

    return_list = []

    try:
        brokers = zk.get_children('/brokers/ids')
        for broker in brokers:
            data, stat = zk.get('/brokers/ids/' +
                                broker)
            t_dict = json.loads(data.decode("utf-8"))
            t_dict["id"] = broker
            t_dict["timestamp_format"] = datetime.datetime.fromtimestamp(
                int(t_dict["timestamp"]) / 1000
            ).strftime('%Y-%m-%d %H:%M:%S')

            t_dict["metrics"] = {}
            t_dict["metrics"][
                "MessagesPerSecOneMinuteRate"] = format(
                float(jmx.get_metric_per_broker(
                    t_dict["host"],
                    jmx.key_messages_in_per_broker,
                    'OneMinuteRate')), '.2f')

            t_dict["version"] = jmx.get_kafka_version(
                t_dict["host"],
                t_dict["id"])

            return_list.append(t_dict)
    except NoNodeError:
        return []

    return return_list
