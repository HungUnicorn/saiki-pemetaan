import json
import logging
import time

from rebalance_partitions import get_zk_dict, generate_json, \
    NotEnoughBrokersException, write_json_to_zk

from controllers.broker import get_brokers, get_jmx_metrics_for_topic
from controllers.general import get_html_tooltip
from kazoo.client import NoNodeError, NodeExistsError
from zookeeper import get_namespace_kafka
from zookeeper import init_zk

namespace_kafka = get_namespace_kafka()


def get_raw_topics(zk):
    """Docstring."""
    return_list = []
    topics = zk.get_children('/brokers/topics')
    for topic in topics:
        t_dict = {"topic_name": topic, 'partitions': {}}
        data, stat = zk.get('/brokers/topics/' +
                            topic)
        try:
            tmp_data_dict = json.loads(data.decode("utf-8"))[
                'partitions']
            for partition_key, brokers in tmp_data_dict.items():
                t_dict["partitions"][partition_key] = {'broker': brokers}
        except json.decoder.JSONDecodeError:
            t_dict["partitions"]['error'] = {'broker': 'error'}

        return_list.append(t_dict)
    return return_list


def get_topics_deletion(zk):
    """Docstring."""
    return zk.get_children('/admin/delete_topics')


def get_topics():
    """Docstring."""
    zk = init_zk(namespace_kafka)

    return_list = []

    brokers = get_brokers()

    try:
        topics = get_raw_topics(zk)
    except NoNodeError:
        return [{'error': 'ZK: NoNodeError'}]

    for topic in topics:
        for partition_key, broker_dict in topic['partitions'].items():
            try:
                data, stat = zk.get('/brokers/topics/' +
                                    topic['topic_name'] + '/partitions/' +
                                    partition_key + '/state')
                topic["partitions"][partition_key]["state"] = json.loads(
                    data.decode("utf-8"))
            except NoNodeError:
                topic["partitions"][partition_key]["state"] = {'isr': 'n/a',
                                                               'leader': 'n/a'}

        # dict for tooltip
        topic["partitions_pretty"] = get_html_tooltip(topic)

        topic["metrics"] = get_jmx_metrics_for_topic(topic['topic_name'],
                                                     brokers)

        topic['delete'] = False
        if topic['topic_name'] in get_topics_deletion(zk):
            topic['delete'] = True

        return_list.append(topic)

    return return_list


def get_config(topic):
    """Docstring."""
    zk = init_zk(namespace_kafka)
    try:
        config_data, stat = zk.get('/config/topics/' + topic)
        config_dict = json.loads(config_data.decode("utf-8"))['config']
    except NoNodeError:
        return {}
    return config_dict


def update_config(cform):
    """Docstring."""
    zk = init_zk(namespace_kafka)

    topic = cform.topic.data
    config_dict = {'version': 1, 'config': {}}
    # all kafka configs are string right now
    if cform.retention_ms.data is not None:
        config_dict['config']['retention.ms'] = str(cform.retention_ms.data)
    if cform.max_message_bytes.data is not None:
        config_dict['config'][
            'max.message.bytes'] = str(cform.max_message_bytes.data)
    if cform.cleanup_policy.data is not None:
        config_dict['config'][
            'cleanup.policy'] = str(cform.cleanup_policy.data)
    if cform.delete_retention_ms.data is not None:
        config_dict['config'][
            'delete.retention.ms'] = str(cform.delete_retention_ms.data)
    if cform.flush_messages.data is not None:
        config_dict['config'][
            'flush.messages'] = str(cform.flush_messages.data)
    if cform.flush_ms.data is not None:
        config_dict['config']['flush.ms'] = str(cform.flush_ms.data)
    if cform.index_interval_bytes.data is not None:
        config_dict['config'][
            'index.interval.bytes'] = str(cform.index_interval_bytes.data)
    if cform.min_cleanable_dirty_ratio.data is not None:
        config_dict['config'][
            'min.cleanable.dirty.ratio'] = str(
                cform.min_cleanable_dirty_ratio.data)
    if cform.min_insync_replicas.data is not None:
        config_dict['config'][
            'min.insync.replicas'] = str(cform.min_insync_replicas.data)
    if cform.retention_bytes.data is not None:
        config_dict['config'][
            'retention.bytes'] = str(cform.retention_bytes.data)
    if cform.segment_index_bytes.data is not None:
        config_dict['config'][
            'segment.index.bytes'] = str(cform.segment_index_bytes.data)
    if cform.segment_bytes.data is not None:
        config_dict['config']['segment.bytes'] = str(cform.segment_bytes.data)
    if cform.segment_ms.data is not None:
        config_dict['config']['segment.ms'] = str(cform.segment_ms.data)
    if cform.segment_jitter_ms.data is not None:
        config_dict['config']['segment.jitter.ms'] = str(
            cform.segment_jitter_ms.data)

    change_topic_config(zk=zk, topic=topic, config_dict=config_dict)


def change_topic_config(zk, topic, config_dict):
    """
    Update the config for an existing topic and create a change notification
    so the change will propagate to other brokers
    """
    update_topic_config(zk, topic, config_dict)
    notify_topic_config_change(zk, topic, config_dict)
    logging.info("created/updated topic config: topic: " + topic +
                 " , config : " + str(config_dict))


def update_topic_config(zk, topic, config_dict):
    try:
        zk.create('/config/topics/' + topic,
                  json.dumps(config_dict).encode('utf-8'), makepath=True)
    except NodeExistsError:
        zk.set('/config/topics/' + topic,
               json.dumps(config_dict).encode('utf-8'))


def notify_topic_config_change(zk, topic, config_dict):
    node = '/config/changes/config_change_' + str(int(time.time()))
    content = {'version': 1,
               'entity_type': 'topics',
               'entity_name': topic}
    try:
        zk.create(node, json.dumps(content).encode('utf-8'), makepath=True)
    except NodeExistsError:
        logging.info("fail to create topic change: " + topic +
                     " , config : " + str(content))


def create_topic_entry(topic_name, partition_count, replication_factor):
    """Docstring."""
    zk = init_zk(namespace_kafka)
    zk_dict = get_zk_dict(zk)
    topics = {topic_name: {}}
    for i in range(0, int(partition_count)):
        topics[topic_name][i] = [0]
        for j in range(1, int(replication_factor)):
            topics[topic_name][i].append(j)
    topic_dict = generate_json(zk_dict,
                               topics_to_reassign=topics)
    new_topic_dict = {'version': topic_dict['version']}
    new_topic_dict['partitions'] = {}
    for partition in topic_dict['partitions']:
        new_topic_dict['partitions'][
            partition['partition']] = partition['replicas']
    zk.create('/brokers/topics/' + topic_name,
              json.dumps(new_topic_dict).encode('utf-8'),
              makepath=True)
    logging.info("created topic : " + topic_name)


def delete_topic_entry(topic_name):
    """Docstring."""
    zk = init_zk(namespace_kafka)
    zk.create('/admin/delete_topics/' + topic_name,
              makepath=True)
    logging.info("marked topic for deletion: " + topic_name)


def validate_topic(topic):
    """Docstring."""
    number_of_whitespace = len(topic) - len(topic.strip())
    zk = init_zk(namespace_kafka)
    if zk.exists('/brokers/topics/' + topic) is not None \
            and number_of_whitespace == 0:
        return True
    else:
        return False


def reassign_all_topics(brokers):
    """Docstring."""
    zk = init_zk(namespace_kafka)
    zk_dict = get_zk_dict(zk)
    logging.info(zk_dict)
    logging.info(brokers)
    try:
        json = generate_json(zk_dict, target_brokers=brokers)
    except NotEnoughBrokersException:
        return 'error: NotEnoughBrokersException'
    import threading
    logging.info(json)
    t = threading.Thread(target=write_json_to_zk, args=(zk, json), daemon=True)
    t.start()
