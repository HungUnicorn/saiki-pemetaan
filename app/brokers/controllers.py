# Import flask dependencies
from flask import Blueprint, flash
from app.zookeeper import init_zk, get_namespace_kafka

from app.auth import check_and_render
from app.settings.controllers import get_settings

import logging

import datetime
import json
from kazoo.client import NoNodeError

from app import jmx

namespace_kafka = get_namespace_kafka()

# Define the blueprint: 'brokers', set its url prefix: app.url/brokers
mod_brokers = Blueprint('brokers', __name__, url_prefix='/brokers')


@mod_brokers.route('/', methods=('GET', 'POST'))
def brokers():
    """Docstring."""
    broker_list = get_brokers()
    # super ugly exception catching , needs to be rewritten
    try:
        if 'error' not in broker_list[0]:
            return check_and_render('brokers/index.html',
                                    display_settings=get_settings(),
                                    brokers=broker_list)
        else:
            logging.warning('There was an error in getting the brokers: ' +
                            broker_list[0]['error'])
            flash('There was an error in getting the brokers: ' +
                  broker_list[0]['error'],
                  'critical')
            return check_and_render('brokers/index.html',
                                    display_settings=get_settings(),
                                    brokers=[])
    except IndexError:
        return check_and_render('brokers/index.html',
                                display_settings=get_settings(),
                                brokers=[])


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
