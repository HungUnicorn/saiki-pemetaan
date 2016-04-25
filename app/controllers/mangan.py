import os
import redis
from kazoo.client import NoNodeError
from zookeeper import init_zk, get_namespace_mangan

namespace_mangan = get_namespace_mangan()
r = redis.StrictRedis(host=os.getenv('REDIS_HOST'), port=6379, db=0)


def get_mangan_settings():
    """Get Mangan Settings for first overview."""
    zk = init_zk(namespace_mangan)
    return_dict = {}

    try:
        c_groups = zk.get_children('/consumer_groups')
    except NoNodeError:
        zk.create('/consumer_groups',
                  makepath=True)
        c_groups = []
    for c_group in c_groups:
        try:
            return_dict[c_group] = zk.get_children('/consumer_groups/' +
                                                   c_group +
                                                   '/event_types')
        except NoNodeError:
            zk.create('/consumer_groups/' + c_group + '/event_types',
                      makepath=True)
            return_dict[c_group] = []
            continue
    return return_dict


def create_mangan_consumer_group(c_group_name):
    """Create Mangan Consumer Group in Zookeeper."""
    zk = init_zk(namespace_mangan)
    zk.create('/consumer_groups/' + c_group_name,
              makepath=True)


def create_mangan_event_type(cg, et):
    """Create Mangan Event Type for a specific Consumer Group in Zookeeper."""
    zk = init_zk(namespace_mangan)
    zk.create('/consumer_groups/' + cg + '/event_types/' + et,
              makepath=True)


def delete_mangan_event_type(cg, et):
    """Delete Mangan Event Type for a specific Consumer Group in Zookeeper."""
    zk = init_zk(namespace_mangan)
    zk.delete('/consumer_groups/' + cg + '/event_types/' + et)


def get_mangan_offsets(et):
    """Get the Offsets from Redis."""
    redis_keys = r.keys(et + "*")
    return_dict = {}
    for key in redis_keys:
        return_dict[key.decode('utf-8')] = r.get(key).decode('utf-8')
    return return_dict


def set_mangan_offset(key, value):
    """Set the Offsets in Redis."""
    r.set(key, value)
