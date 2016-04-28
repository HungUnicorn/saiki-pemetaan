# Import flask dependencies
from flask import Blueprint, flash, redirect, request, url_for
from app.zookeeper import init_zk, get_namespace_kafka
from app.topics.rebalance_partitions import get_zk_dict, generate_json, \
    NotEnoughBrokersException, write_json_to_zk
from kazoo.client import NodeExistsError
import time

from app.auth import check_and_render, only_check
from app.settings.controllers import get_settings

import logging

import json
from kazoo.client import NoNodeError

from app.topics.forms import ConfigForm, TopicForm, MultiCheckboxField
from flask_wtf import Form

from app.brokers.controllers import get_brokers

from app import jmx

namespace_kafka = get_namespace_kafka()

# Define the blueprint: 'brokers', set its url prefix: app.url/brokers
mod_topics = Blueprint('topics', __name__, url_prefix='/topics')


@mod_topics.route('/', methods=('GET', 'POST'))
def topics():
    """Docstring."""
    topic_list = get_topics()
    # super ugly exception catching , needs to be rewritten
    try:
        if 'error' not in topic_list[0]:
            return check_and_render('topics/index.html',
                                    display_settings=get_settings(),
                                    topics=topic_list)
        else:
            logging.warning('There was an error in getting the topics: ' +
                            topic_list[0]['error'])
            flash('There was an error in getting the topics: ' +
                  topic_list[0]['error'],
                  'critical')
            return check_and_render('topics/index.html',
                                    display_settings=get_settings(),
                                    topics=[])
    except IndexError:
        return check_and_render('index.html',
                                display_settings=get_settings(),
                                topics=[])


def config_convert_to_python(config_dict):
    """Docstring."""
    return_dict = {}
    for key, value in config_dict.items():
        return_dict[key.replace('.', '_')] = value
    return return_dict


@mod_topics.route('/config', methods=('GET', 'POST'))
def topics_config():
    """Docstring."""
    if only_check():
        if request.method == 'POST':
            cform = ConfigForm()
            cform.validate_on_submit()  # to get error messages to the browser
            update_config(cform)
            flash('updated Config for Topic : ' + cform.topic.data)
            return redirect(url_for('topics'))
        elif request.method == 'GET':
            topic_name = request.args.get('topic')
            config = get_config(topic_name)
            conv_config = config_convert_to_python(config)
            cform = ConfigForm(topic=topic_name, **conv_config)
            return check_and_render('topics/config.html',
                                    display_settings=get_settings(),
                                    form=cform)
    else:
        return check_and_render('index.html', display_settings=get_settings())


@mod_topics.route('/create', methods=('GET', 'POST'))
def create_topic():
    """Docstring."""
    if only_check():
        tform = TopicForm()
        tform.validate_on_submit()  # to get error messages to the browser
        if request.method == 'POST':
            if tform.validate() is False:
                flash('Please check that all the fields are valid.',
                      'critical')
                return check_and_render('topics/create.html',
                                        display_settings=get_settings(),
                                        form=tform)
            else:
                if validate_topic(tform.topic_name.data) is False:
                    create_topic_entry(tform.topic_name.data,
                                       tform.partition_count.data,
                                       tform.replication_factor.data)
                    flash('Added Topic: ' +
                          tform.topic_name.data)
                    return redirect(url_for('topics.topics'))
                else:
                    flash('This topic name exists already.', 'critical')
                    return check_and_render('topics/create.html',
                                            display_settings=get_settings(),
                                            form=tform)
        elif request.method == 'GET':
            return check_and_render('topics/create.html',
                                    display_settings=get_settings(),
                                    form=tform)
    else:
        return check_and_render('index.html', display_settings=get_settings())


@mod_topics.route('/move', methods=('GET', 'POST'))
def move_topics():
    """Docstring."""
    class MoveForm(Form):
        brokers = get_brokers()
        my_choices = []
        for broker in brokers:
            my_choices.append((broker['id'], 'ID: ' +
                               str(broker['id']) + ', Host: ' +
                               str(broker['host'])))

        brokers_select_fields = MultiCheckboxField("Select",
                                                   choices=my_choices,
                                                   coerce=int)

    if only_check():
        moveform = MoveForm()
        moveform.validate_on_submit()

        if request.method == 'POST':

            str_brokers = []
            for broker in moveform.brokers_select_fields.data:
                str_brokers.append(str(broker))
            result = reassign_all_topics(str_brokers)
            if not isinstance(result, str):
                return redirect(url_for('topics'))
            else:
                flash(result, 'critical')
                return check_and_render('topics/move.html',
                                        display_settings=get_settings(),
                                        form=moveform)
        elif request.method == 'GET':
            return check_and_render('topics/move.html',
                                    display_settings=get_settings(),
                                    form=moveform)

    else:
        return check_and_render('index.html', display_settings=get_settings())


@mod_topics.route('/delete', methods=('GET', 'POST'))
def delete_topic():
    """Docstring."""
    if only_check():
        topic = request.args.get('topic')
        if validate_topic(topic) is True:
            delete_topic_entry(topic)
            flash('Deleted Topic: ' + topic)
        return redirect(url_for('topics.topics'))
    else:
        check_and_render('index.html', display_settings=get_settings())


def get_raw_topics(zk):
    """Docstring."""
    return_list = []
    topic_list = zk.get_children('/brokers/topics')
    for topic in topic_list:
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
        topic_list = get_raw_topics(zk)
    except NoNodeError:
        return [{'error': 'ZK: NoNodeError'}]

    for topic in topic_list:
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
    notify_topic_config_change(zk, topic)
    logging.info("created/updated topic config: topic: " + topic +
                 " , config : " + str(config_dict))


def update_topic_config(zk, topic, config_dict):
    try:
        zk.create('/config/topics/' + topic,
                  json.dumps(config_dict).encode('utf-8'), makepath=True)
    except NodeExistsError:
        zk.set('/config/topics/' + topic,
               json.dumps(config_dict).encode('utf-8'))


def notify_topic_config_change(zk, topic):
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
    new_topic_dict = {topic_name: {}}
    for i in range(0, int(partition_count)):
        new_topic_dict[topic_name][i] = [0]
        for j in range(1, int(replication_factor)):
            new_topic_dict[topic_name][i].append(j)
    topic_dict = generate_json(zk_dict,
                               topics_to_reassign=new_topic_dict)
    new_topic_dict_zk = {'version': topic_dict['version'], 'partitions': {}}
    for partition in topic_dict['partitions']:
        new_topic_dict_zk['partitions'][
            partition['partition']] = partition['replicas']
    zk.create('/brokers/topics/' + topic_name,
              json.dumps(new_topic_dict_zk).encode('utf-8'),
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
        json_string = generate_json(zk_dict, target_brokers=brokers)
    except NotEnoughBrokersException:
        return 'error: NotEnoughBrokersException'
    import threading
    logging.info(json_string)
    t = threading.Thread(target=write_json_to_zk,
                         args=(zk, json_string),
                         daemon=True)
    t.start()


def get_html_tooltip(t_dict):
    """Docstring."""
    return_string = '<table>'
    for partition_key, value in t_dict["partitions"].items():
        return_string += '<tr><td>'
        return_string += partition_key + ' : ' + str(
            t_dict["partitions"][partition_key][
                'broker']) + '</td>'
        return_string += '<td>Leader: ' + str(t_dict[
            "partitions"][partition_key][
            "state"]["leader"]) + '</td>'
        return_string += '<td>ISR: ' + str(t_dict[
            "partitions"][partition_key][
            "state"]["isr"]) + '</td><tr>'
    return_string += '</table>'
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
