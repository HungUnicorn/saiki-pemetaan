# coding=utf-8
"""Controller File."""
from zookeeper import init_zk, get_namespace_kafka, get_namespace_saiki
from kazoo.client import NoNodeError, NodeExistsError
import json
from rebalance_partitions import get_zk_dict, generate_json, \
    NotEnoughBrokersException, write_json_to_zk
import logging
import datetime
import urllib
import jmx

namespace = get_namespace_kafka()
namespace_saiki = get_namespace_saiki()


def get_html_tooltip(t_dict):
    """Docstring."""
    return_string = '<table>'
    for partition_key, value in t_dict["partitions"].items():
        return_string = return_string + '<tr><td>'
        return_string = return_string + partition_key + ' : ' + str(
            t_dict["partitions"][partition_key][
                'broker']) + '</td>'
        return_string = return_string + '<td>Leader: ' + str(t_dict[
            "partitions"][partition_key][
                "state"]["leader"]) + '</td>'
        return_string = return_string + '<td>ISR: ' + str(t_dict[
            "partitions"][partition_key][
                "state"]["isr"]) + '</td><tr>'
    return_string = return_string + '</table>'
    return return_string


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
    zk = init_zk(namespace)

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
    zk = init_zk(namespace)
    try:
        config_data, stat = zk.get('/config/topics/' + topic)
        config_dict = json.loads(config_data.decode("utf-8"))['config']
    except NoNodeError:
        return {}
    return config_dict


def update_config(cform):
    """Docstring."""
    zk = init_zk(namespace)

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

    try:
        zk.create('/config/topics/' + topic,
                  json.dumps(config_dict).encode('utf-8'),
                  makepath=True)
    except NodeExistsError:
        zk.set('/config/topics/' + topic,
               json.dumps(config_dict).encode('utf-8'))

    logging.info("created/updated topic config: topic: " + topic +
                 " , config : " + str(config_dict))


def create_topic_entry(topic_name, partition_count, replication_factor):
    """Docstring."""
    zk = init_zk(namespace)
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
    zk = init_zk(namespace)
    zk.create('/admin/delete_topics/' + topic_name,
              makepath=True)
    logging.info("marked topic for deletion: " + topic_name)


def validate_topic(topic):
    """Docstring."""
    zk = init_zk(namespace)
    if zk.exists('/brokers/topics/' + topic) is not None:
        return True
    else:
        return False


def reassign_all_topics(brokers):
    """Docstring."""
    zk = init_zk(namespace)
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


def get_raw_brokers():
    """Docstring."""
    pass


def get_brokers():
    """Docstring."""
    zk = init_zk(namespace)

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


def get_mappings():
    """Docstring."""
    zk = init_zk(namespace_saiki)
    return_list = []

    try:
        c_ids = zk.get_children('/content_types')
    except NoNodeError:
        logging.error("no node error: zk.get_children('" +
                      "/content_types')")
        return return_list
    for c_id in c_ids:
        c_id_dec = urllib.parse.unquote(c_id)
        t_dict = {"c_name": c_id_dec, "c_name_enc": c_id, "topics": []}
        try:
            topics = zk.get_children('/content_types/' +
                                     c_id +
                                     '/topics')
        except NoNodeError:
            logging.error("no node error: zk.get_children('" +
                          "/content_types/" + c_id + "/topics')")
            topics = []
            logging.warning("there is a broken topic mapping! CT: " +
                            c_id +
                            ", Topic: <WRONG_TOPIC_SPECIFICATION>")
            t_dict["topics"].append({"name": "<WRONG_TOPIC_SPECIFICATION>",
                                     "data": "false",
                                     "error": "true"})
        if len(topics) == 0:
            logging.warning("there is a broken topic mapping! CT: " +
                            c_id +
                            ", Topic: <NO_TOPIC_SPECIFIED>")
            t_dict["topics"].append({"name": "<NO_TOPIC_SPECIFIED>",
                                     "data": "false",
                                     "error": "true"})
        for topic in topics:
            data, stat = zk.get('/content_types/' +
                                c_id +
                                '/topics/' +
                                topic)
            try:
                t_dict["topics"].append({"name": topic,
                                         "data": json.loads(
                                             data.decode("utf-8")
                                         )})
            except json.decoder.JSONDecodeError:
                logging.warning("there is a broken topic mapping! CT: " +
                                c_id +
                                ", Topic: " + topic)
                t_dict["topics"].append({"name": topic,
                                         "data": "false",
                                         "error": "true"})
        return_list.append(t_dict)

    return return_list


def write_mapping(ct, topic, active):
    """Docstring."""
    zk = init_zk(namespace_saiki)

    ct_enc = urllib.parse.quote(ct, safe='')
    zk.create('/content_types/' + ct_enc + '/topics/' + topic,
              json.dumps({'active': active}).encode('utf-8'),
              makepath=True)
    logging.info("created topic mapping : CT: " + ct_enc + ", Topic: " +
                 topic + ", data: " + json.dumps({'active': active}))


def delete_mapping(ct, topic):
    """Docstring."""
    zk = init_zk(namespace_saiki)

    ct_enc = urllib.parse.quote(ct, safe='')
    delete_whole = False
    try:
        topics = zk.get_children('/content_types/' +
                                 ct_enc +
                                 '/topics')
    except NoNodeError:
        logging.warning("no node error: zk.get_children('" +
                        "/content_types')")
        topics = []
    if len(topics) == 0:
        delete_whole = True
    if delete_whole:
        logging.warning("deleting whole content-type because its broken: " +
                        ct_enc)
        zk.delete('/content_types/' + ct_enc,
                  recursive=True)
    else:
        logging.info("deleting mapping: CT: " + ct_enc + ", Topic: " + topic)
        zk.delete('/content_types/' +
                  ct_enc +
                  '/topics/' +
                  topic,
                  recursive=True)
        try:
            topics = zk.get_children('/content_types/' +
                                     ct_enc +
                                     '/topics')
        except NoNodeError:
            logging.warning("no node error: zk.get_children('" +
                            "/content_types')")
            topics = []
        if len(topics) == 0:
            logging.warning("deleting whole content-type: " +
                            ct_enc)
            zk.delete('/content_types/' + ct_enc,
                      recursive=True)


def get_saiki_templates():
    """Docstring."""
    zk = init_zk(namespace_saiki)

    try:
        return zk.get_children('/templates')
    except NoNodeError:
        return []


def get_saiki_template_single(template):
    """Docstring."""
    zk = init_zk(namespace_saiki)

    try:
        data, stat = zk.get('/templates/' + template)
        return urllib.parse.unquote(data.decode('utf-8'))
    except NoNodeError:
        return [{'error': 'ZK: NoNodeError'}]


def update_template(template_form):
    """Docstring."""
    zk = init_zk(namespace_saiki)
    template = template_form.template_name.data
    template_data = template_form.template_data.data.encode()
    try:
        zk.create('/templates/' + template, template_data)
        return True
    except NodeExistsError:
        zk.set('/templates/' + template, template_data)
        return True


def delete_template(template):
    """Docstring."""
    zk = init_zk(namespace_saiki)
    zk.delete('/templates/' + template)
