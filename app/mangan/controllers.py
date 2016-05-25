from app.zookeeper import init_zk, get_namespace_mangan
from kazoo.client import NoNodeError, NodeExistsError
import redis
import os
import json
from flask import Blueprint, flash, request, redirect, url_for
from app.auth import check_and_render, only_check
from app.mangan.forms import ConsumerGroupForm, ManganEventTypeForm
from app.settings.controllers import get_settings

r = redis.StrictRedis(host=os.getenv('REDIS_HOST'), port=6379, db=0)

namespace_mangan = get_namespace_mangan()
mod_mangan = Blueprint('mangan', __name__, url_prefix='/mangan')


@mod_mangan.route('/')
def mangan_index():
    """Mangan Index Page."""
    if only_check():
        delete_et = request.args.get('delete_et')
        c_group = request.args.get('c_group')
        if delete_et is not None and delete_et != '':
            delete_mangan_event_type(c_group, delete_et)
            flash('deleted Event Type ' +
                  delete_et +
                  ' in Consumer Group ' +
                  c_group)
        return check_and_render('mangan/index.html',
                                display_settings=get_settings(),
                                c_group_param=c_group,
                                mangan_settings=get_mangan_settings())
    else:
        return check_and_render('index.html', display_settings=get_settings())


@mod_mangan.route('/create/consumer_group', methods=('GET', 'POST'))
def create_mangan_consumer_group_page():
    """Display Mangan Consumer Group Creation Page."""
    if only_check():
        if request.method == 'POST':
            c_group_form = ConsumerGroupForm()
            if c_group_form.validate() is False:
                flash('Please check that all the fields are valid!.',
                      'critical')
                return check_and_render('mangan/create_c_group.html',
                                        display_settings=get_settings(),
                                        form=c_group_form)
            else:
                create_mangan_consumer_group(c_group_form.consumer_group.data)
                flash('created Consumer Group: ' +
                      c_group_form.consumer_group.data)
                return redirect(url_for('mangan_index') +
                                "?c_group=" +
                                c_group_form.consumer_group.data)
        elif request.method == 'GET':
            c_group_form = ConsumerGroupForm()
            return check_and_render('mangan/create_c_group.html',
                                    display_settings=get_settings(),
                                    form=c_group_form)
    else:
        return check_and_render('index.html',
                                display_settings=get_settings())


@mod_mangan.route('/create/event_type', methods=('GET', 'POST'))
def create_mangan_event_type_page():
    """Display Mangan Consumer Group Creation Page."""
    if only_check():
        if request.method == 'POST':
            et_form = ManganEventTypeForm()
            print(et_form)
            if et_form.validate() is False:
                flash('Please check that all the fields are valid!.',
                      'critical')
                return check_and_render('mangan/create_et.html',
                                        display_settings=get_settings(),
                                        form=et_form)
            else:
                if create_mangan_event_type(et_form.cg.data,
                                            et_form.et.data,
                                            et_form.et_regex.data,
                                            et_form.batch_size.data,
                                            et_form.nakadi_endpoint.data):
                    flash('created Event Type ' +
                          et_form.et.data +
                          ' in Consumer Group ' +
                          et_form.cg.data)
                    return redirect(url_for('mangan.mangan_index') +
                                    "?c_group=" +
                                    et_form.cg.data)
                else:
                    flash('Please check that all the fields are valid!.',
                          'critical')
                    return check_and_render('mangan/create_et.html',
                                            display_settings=get_settings(),
                                            form=et_form)
        elif request.method == 'GET':
            cg = request.args.get('c_group')
            et_form = ManganEventTypeForm(cg=cg)
            return check_and_render('mangan/create_et.html',
                                    display_settings=get_settings(),
                                    form=et_form,
                                    cg=cg)
    else:
        return check_and_render('index.html', display_settings=get_settings())


@mod_mangan.route('/offsets')
def mangan_offsets():
    """Investigate on Mangan Offsets."""
    if only_check():
        et = request.args.get('et')
        offsets = get_mangan_offsets(et)
        return check_and_render('mangan/offsets.html',
                                display_settings=get_settings(),
                                et=et,
                                offsets=offsets)
    else:
        return check_and_render('index.html', display_settings=get_settings())


@mod_mangan.route('/offsets/set')
def mangan_set_offset():
    """Investigate on Mangan Offsets."""
    if only_check():
        key = request.args.get('key')
        value = request.args.get('value')
        set_mangan_offset(key, value)
        offsets = get_mangan_offsets(key[:-1])
        return check_and_render('mangan/offsets.html',
                                display_settings=get_settings(),
                                et=key[:-1],
                                offsets=offsets)
    else:
        return check_and_render('index.html',
                                display_settings=get_settings())


def get_mangan_settings():
    """Get Mangan Settings for first overview."""
    zk = init_zk(namespace_mangan)
    return_dict = {}

    try:
        c_groups = zk.get_children('/consumer_groups')
    except NoNodeError:
        zk.create('/consumer_groups',
                  makepath=True)
        c_groups = {}
    for c_group in c_groups:
        return_dict[c_group] = []
        try:
            for et_pattern_name in zk.get_children('/consumer_groups/' +
                                                   c_group +
                                                   '/event_types'):
                data, stat = zk.get('/consumer_groups/' +
                                    c_group +
                                    '/event_types/' + et_pattern_name)
                et_pattern_data = json.loads(data.decode("utf-8"))
                et_pattern_data['name'] = et_pattern_name
                et_pattern_data['regex_clean'] = et_pattern_data['regex'].\
                    replace('\.', '.').replace('^', '').replace('.*', '')
                return_dict[c_group].append(et_pattern_data)
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


def create_mangan_event_type(cg, et, et_regex, batch_size, nakadi_endpoint):
    """Create Mangan Event Type for a specific Consumer Group in Zookeeper."""
    zk = init_zk(namespace_mangan)
    try:
        zk.create('/consumer_groups/' + cg + '/event_types/' + et,
                  json.dumps({'regex': et_regex,
                              'batch_size': int(batch_size),
                              'nakadi_endpoint': nakadi_endpoint}).
                  encode('UTF-8'),
                  makepath=True)
        return True
    except NodeExistsError:
        flash("This name exists already!", "critical")
        return False


def delete_mangan_event_type(cg, et):
    """Delete Mangan Event Type for a specific Consumer Group in Zookeeper."""
    zk = init_zk(namespace_mangan)
    zk.delete('/consumer_groups/' + cg + '/event_types/' + et)


def get_mangan_offsets(et):
    """Get the Offsets from Redis."""
    return_dict = {}
    try:
        redis_keys = r.keys(et + "*")
    except redis.exceptions.ConnectionError:
        flash("Could not get the values from Redis, " +
              "maybe check the connection?", 'critical')
        return return_dict
    for key in redis_keys:
        return_dict[key.decode('utf-8')] = r.get(key).decode('utf-8')
    return return_dict


def set_mangan_offset(key, value):
    """Set the Offsets in Redis."""
    r.set(key, value)
