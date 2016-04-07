# coding=utf-8
"""File to handly JMX calls."""
import logging

key_kafka_version = "kafka.server:type=app-info,id="
key_kafka_version_alt = "kafka.common:name=Version," \
                        + "type=AppInfo"

key_messages_in_per_broker = "kafka.server:name=MessagesInPerSec," \
                             + "type=BrokerTopicMetrics"
key_messages_in_per_topic = "kafka.server:name=MessagesInPerSec," \
                            + "type=BrokerTopicMetrics," \
                            + "topic="
key_bytes_in_per_topic = "kafka.server:name=BytesInPerSec," \
                         + "type=BrokerTopicMetrics," \
                         + "topic="


def get_kafka_version(broker, broker_id):
    """Get the kafka version for this broker."""
    version = get_metric_per_broker(broker,
                                    key_kafka_version + broker_id,
                                    "Version")
    if version == "":
        version = get_metric_per_broker(broker, key_kafka_version_alt, "Value")
    return version


def get_metric_per_broker(broker, metric, value_key=None):
    """Get a kafka metric."""
    import requests
    import json

    try:
        response = json.loads(requests.get('http://' +
                                           broker + ':8778/jolokia/read/' +
                                           metric).text)
    except requests.exceptions.ConnectionError:
        logging.warning("can't reach " +
                        broker + ':8778 for jmx metrics rest endpoint ...')
        return "-1"
    if 'error' not in response:
        if value_key is not None:
            return response['value'][value_key]
        else:
            return response['value']
    else:
        logging.warning("can't get jmx metrics from %s:8778/jolokia/read/%s. Response: %s", broker, metric, response)
        return "-1"
