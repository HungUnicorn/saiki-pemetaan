import urllib
from app.zookeeper import init_zk, get_namespace_saiki
from app.topics.controllers import validate_topic
from app.settings.controllers import get_settings

# Import flask dependencies
from flask import Blueprint, flash, redirect, url_for, request
from app.topic_mapping.forms import MappingForm

from app.auth import check_and_render, only_check

import logging

import json
from kazoo.client import NoNodeError

namespace_saiki = get_namespace_saiki()

mod_topic_mapping = Blueprint('topic_mapping',
                              __name__,
                              url_prefix='/topic_mapping')


@mod_topic_mapping.route('/', methods=('GET', 'POST'))
def topic_mapping():
    """Docstring."""
    mappings = get_mappings()
    # super ugly exception catching , needs to be rewritten
    try:
        if 'error' not in mappings[0]:
            return check_and_render('topic_mapping/index.html',
                                    display_settings=get_settings(),
                                    mappings=mappings)
        else:
            logging.warning('There was an error in getting ' +
                            'the topic mappings: ' +
                            mappings[0]['error'])
            flash('There was an error in getting the topic mappings: ' +
                  mappings[0]['error'],
                  'critical')
            return check_and_render('topic_mapping/index.html',
                                    display_settings=get_settings(),
                                    mappings=[])
    except IndexError:
        return check_and_render('topic_mapping/index.html',
                                display_settings=get_settings(),
                                mappings=[])


@mod_topic_mapping.route('/create', methods=('GET', 'POST'))
def create_topic_mapping():
    """Docstring."""
    if only_check():
        mform = MappingForm()
        mform.validate_on_submit()  # to get error messages to the browser
        if request.method == 'POST':
            if mform.validate() is False:
                flash('Please check that all the fields are valid.',
                      'critical')
                return check_and_render('topic_mapping/create.html',
                                        display_settings=get_settings(),
                                        form=mform)
            else:
                if validate_topic(mform.topic.data) is True:
                    write_mapping(mform.content_type.data,
                                  mform.topic.data,
                                  mform.active.data)
                    flash('Added Mapping: ' +
                          mform.content_type.data +
                          ' <> ' +
                          mform.topic.data)
                    return redirect(url_for('topic_mapping.topic_mapping'))
                else:
                    flash('This topic does not exist!',
                          'critical')
                    return check_and_render('topic_mapping/create.html',
                                            display_settings=get_settings(),
                                            form=mform)
        elif request.method == 'GET':
            return check_and_render('topic_mapping/create.html',
                                    display_settings=get_settings(),
                                    form=mform)
    else:
        return check_and_render('index.html', display_settings=get_settings())


@mod_topic_mapping.route('/delete', methods=('GET', 'POST'))
def delete_topic_mapping():
    """Docstring."""
    if only_check():
        delete_mapping(request.args.get('ct'), request.args.get('topic'))
        flash('Deleted Mapping: ' +
              request.args.get('ct') +
              ' <> ' +
              request.args.get('topic'))
        return redirect(url_for('topic_mapping'))
    else:
        check_and_render('index.html', display_settings=get_settings())


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
    if validate_topic(topic):
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
