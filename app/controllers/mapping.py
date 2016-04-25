"""Controller File."""
import json
import logging
import urllib

from controllers.topic import validate_topic
from kazoo.client import NoNodeError
from zookeeper import init_zk, get_namespace_saiki

namespace_saiki = get_namespace_saiki()


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
