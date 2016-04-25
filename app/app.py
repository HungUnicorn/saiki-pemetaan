# coding=utf-8
"""Main App File."""
import json
import logging

import os
from flask import Flask, flash, request, redirect, url_for, \
    session
from flask_bootstrap import Bootstrap
from flask_wtf import Form

# import uwsgi_metrics
from forms import MappingForm, TopicForm, ConfigForm, MultiCheckboxField, \
    TemplateForm, ConsumerGroupForm, ManganEventTypeForm
from controllers.general import get_settings, update_settings

from controllers.mapping import get_mappings, write_mapping, delete_mapping

from controllers.topic import get_topics, create_topic_entry, validate_topic, \
    delete_topic_entry, reassign_all_topics, get_config, update_config

from controllers.broker import get_brokers

from controllers.template import get_saiki_templates, \
    get_saiki_template_single, update_template, delete_template

from controllers.mangan import get_mangan_settings, \
    create_mangan_consumer_group, create_mangan_event_type, \
    delete_mangan_event_type, get_mangan_offsets, set_mangan_offset


from security import check_and_render, only_check, get_auth

logging.basicConfig(level=getattr(logging, 'INFO', None))
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("kazoo").setLevel(logging.WARNING)

# from werkzeug.debug import DebuggedApplication


# uwsgi_metrics.initialize()


app = Flask(__name__)


# app.wsgi_app = DebuggedApplication(app.wsgi_app, True)
app.debug = True
app.secret_key = os.getenv('APP_SECRET_KEY', 'development')


auth = get_auth(app)

# AppConfig(app, configfile)
# Flask-Appconfig is not necessary, but
# highly recommend =)
# https://github.com/mbr/flask-appconfig
Bootstrap(app)

# in a real app, these should be configured through Flask-Appconfig
app.config['SECRET_KEY'] = 'devkey'
app.config['RECAPTCHA_PUBLIC_KEY'] = \
    '6Lfol9cSAAAAADAkodaYl9wvQCwBMr3qGR_PPHcw'


@app.template_filter('to_json')
def to_json(value):
    """To_Json function for flask."""
    return json.dumps(value, indent=4)


@app.route('/', methods=('GET', 'POST'))
def index():
    """Main Page."""
    return check_and_render('index.html', force_render=True)


@app.route('/health')
def health():
    """Health Endpoint."""
    return 'OK'


@app.route('/topic_mapping', methods=('GET', 'POST'))
def topic_mapping():
    """Docstring."""
    mappings = get_mappings()
    # super ugly exception catching , needs to be rewritten
    try:
        if 'error' not in mappings[0]:
            return check_and_render('topic_mapping.html', mappings=mappings)
        else:
            logging.warning('There was an error in getting ' +
                            'the topic mappings: ' +
                            mappings[0]['error'])
            flash('There was an error in getting the topic mappings: ' +
                  mappings[0]['error'],
                  'critical')
            return check_and_render('topic_mapping.html', mappings=[])
    except IndexError:
        return check_and_render('topic_mapping.html', mappings=[])


@app.route('/topic_mapping/create', methods=('GET', 'POST'))
def create_topic_mapping():
    """Docstring."""
    if only_check():
        mform = MappingForm()
        mform.validate_on_submit()  # to get error messages to the browser
        if request.method == 'POST':
            if mform.validate() is False:
                flash('Please check that all the fields are valid.',
                      'critical')
                return check_and_render('topic_mapping_create.html',
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
                    return redirect(url_for('topic_mapping'))
                else:
                    flash('This topic does not exist!',
                          'critical')
                    return check_and_render('topic_mapping_create.html',
                                            form=mform)
        elif request.method == 'GET':
            return check_and_render('topic_mapping_create.html',
                                    form=mform)
    else:
        return check_and_render('index.html')


@app.route('/topic_mapping/delete', methods=('GET', 'POST'))
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
        check_and_render('index.html')


@app.route('/topics', methods=('GET', 'POST'))
def topics():
    """Docstring."""
    topics = get_topics()
    # super ugly exception catching , needs to be rewritten
    try:
        if 'error' not in topics[0]:
            return check_and_render('topics.html', topics=topics)
        else:
            logging.warning('There was an error in getting the topics: ' +
                            topics[0]['error'])
            flash('There was an error in getting the topics: ' +
                  topics[0]['error'],
                  'critical')
            return check_and_render('topics.html', topics=[])
    except IndexError:
        return check_and_render('topics.html', topics=[])


def config_convert_to_python(config_dict):
    """Docstring."""
    return_dict = {}
    for key, value in config_dict.items():
        return_dict[key.replace('.', '_')] = value
    return return_dict


@app.route('/topics/config', methods=('GET', 'POST'))
def topics_config():
    """Docstring."""
    if only_check():
        if request.method == 'POST':
            cform = ConfigForm()
            cform.validate_on_submit()  # to get error messages to the browser
            # if cform.validate() is False:
            #     flash('Please check that all the fields are valid!.',
            #           'critical')
            #     return check_and_render('topics_config.html',
            #                             form=cform)
            # else:
            update_config(cform)
            flash('updated Config for Topic : ' + cform.topic.data)
            return redirect(url_for('topics'))
        elif request.method == 'GET':
            topic_name = request.args.get('topic')
            config = get_config(topic_name)
            conv_config = config_convert_to_python(config)
            cform = ConfigForm(topic=topic_name, **conv_config)
            return check_and_render('topics_config.html',
                                    form=cform)
    else:
        return check_and_render('index.html')


@app.route('/topics/create', methods=('GET', 'POST'))
def create_topic():
    """Docstring."""
    if only_check():
        tform = TopicForm()
        tform.validate_on_submit()  # to get error messages to the browser
        if request.method == 'POST':
            if tform.validate() is False:
                flash('Please check that all the fields are valid.',
                      'critical')
                return check_and_render('topics_create.html',
                                        form=tform)
            else:
                if validate_topic(tform.topic_name.data) is False:
                    create_topic_entry(tform.topic_name.data,
                                       tform.partition_count.data,
                                       tform.replication_factor.data)
                    flash('Added Topic: ' +
                          tform.topic_name.data)
                    return redirect(url_for('topics'))
                else:
                    flash('This topic name exists already.', 'critical')
                    return check_and_render('topics_create.html',
                                            form=tform)
        elif request.method == 'GET':
            return check_and_render('topics_create.html',
                                    form=tform)
    else:
        return check_and_render('index.html')


@app.route('/topics/move', methods=('GET', 'POST'))
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
                return check_and_render('topics_move.html',
                                        form=moveform)
        elif request.method == 'GET':
            return check_and_render('topics_move.html',
                                    form=moveform)

    else:
        return check_and_render('index.html')


@app.route('/topics/delete', methods=('GET', 'POST'))
def delete_topic():
    """Docstring."""
    if only_check():
        topic = request.args.get('topic')
        if validate_topic(topic) is True:
            delete_topic_entry(topic)
            flash('Deleted Topic: ' + topic)
        return redirect(url_for('topics'))
    else:
        check_and_render('index.html')


@app.route('/brokers', methods=('GET', 'POST'))
def brokers():
    """Docstring."""
    brokers = get_brokers()
    # super ugly exception catching , needs to be rewritten
    try:
        if 'error' not in brokers[0]:
            return check_and_render('brokers.html', brokers=brokers)
        else:
            logging.warning('There was an error in getting the brokers: ' +
                            brokers[0]['error'])
            flash('There was an error in getting the brokers: ' +
                  brokers[0]['error'],
                  'critical')
            return check_and_render('brokers.html', brokers=[])
    except IndexError:
        return check_and_render('brokers.html', brokers=[])


@app.route('/saiki_templates', methods=('GET', 'POST'))
def saiki_templates():
    """Docstring."""
    templates = get_saiki_templates()
    return check_and_render('saiki_templates.html', templates=templates)


@app.route('/saiki_templates/delete', methods=('GET', 'POST'))
def saiki_templates_delete():
    """Docstring."""
    if only_check():
        template = request.args.get('template')
        delete_template(template)
        flash('Deleted Template: ' + template)
        return redirect(url_for('saiki_templates'))
    else:
        check_and_render('index.html')


@app.route('/saiki_templates/edit', methods=('GET', 'POST'))
def saiki_templates_edit():
    """Docstring."""
    if only_check():
        if request.method == 'POST':
            template_form = TemplateForm()
            template_form.validate_on_submit()
            print(template_form)
            if template_form.validate() is False:
                flash('Please check that all the fields are valid.',
                      'critical')
                return check_and_render('saiki_templates_edit.html',
                                        form=template_form)
            else:
                update_template(template_form)
                flash('updated Config for Topic : ' +
                      template_form.template_name.data)
                return redirect(url_for('saiki_templates'))
        elif request.method == 'GET':
            template = request.args.get('template')
            if template != '' and template is not None:
                template_data = get_saiki_template_single(template)
            else:
                template = ''
                template_data = '{}'
            template_form = TemplateForm(template_name=template,
                                         template_data=template_data)
            return check_and_render('saiki_templates_edit.html',
                                    form=template_form,
                                    template_data=template_data)
    else:
        return check_and_render('index.html')


@app.route('/login', methods=('GET', 'POST'))
def login():
    """Docstring."""
    return auth.authorize(callback=os.getenv('APP_URL', '').rstrip('/') +
                          '/login/authorized')


@app.route('/logout')
def logout():
    """Docstring."""
    session.pop('auth_token', None)
    logging.info("Session logged out: " + str(session))
    flash('Successfully logged out!')
    return redirect(url_for('index'))


@app.route('/login/authorized')
def authorized():
    """Docstring."""
    resp = auth.authorized_response()
    if resp is None:
        return 'Access denied: reason=%s error=%s' % (
            request.args['error'],
            request.args['error_description']
        )
    if not isinstance(resp, dict):
        logging.debug(resp)
        return 'Invalid auth response'
    session['auth_token'] = (resp['access_token'], '')
    logging.info("Session: " + str(session))
    logging.info("resp: " + str(resp))
    logging.info("auth_token: " + str(session['auth_token']))
    return redirect(url_for('index'))


@auth.tokengetter
def get_auth_oauth_token():
    """Docstring."""
    return session.get('auth_token')


@app.route('/settings')
def pemetaan_settings():
    """Creating Settings Overview Page."""
    if only_check():
        setting = request.args.get('setting')
        value = request.args.get('value')
        if setting is not None and value is not None:
            from distutils.util import strtobool
            update_settings(setting, strtobool(value))
            return check_and_render('settings.html',
                                    settings=get_settings())
        else:
            return check_and_render('settings.html',
                                    settings=get_settings())
    else:
        return check_and_render('index.html')


@app.route('/mangan')
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
        return check_and_render('mangan.html',
                                c_group_param=c_group,
                                mangan_settings=get_mangan_settings())
    else:
        return check_and_render('index.html')


@app.route('/mangan/create/consumer_group', methods=('GET', 'POST'))
def create_mangan_consumer_group_page():
    """Display Mangan Consumer Group Creation Page."""
    if only_check():
        if request.method == 'POST':
            c_group_form = ConsumerGroupForm()
            if c_group_form.validate() is False:
                flash('Please check that all the fields are valid!.',
                      'critical')
                return check_and_render('mangan_create_c_group.html',
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
            return check_and_render('mangan_create_c_group.html',
                                    form=c_group_form)
    else:
        return check_and_render('index.html')


@app.route('/mangan/create/event_type', methods=('GET', 'POST'))
def create_mangan_event_type_page():
    """Display Mangan Consumer Group Creation Page."""
    if only_check():
        if request.method == 'POST':
            et_form = ManganEventTypeForm()
            print(et_form)
            if et_form.validate() is False:
                flash('Please check that all the fields are valid!.',
                      'critical')
                return check_and_render('mangan_create_et.html',
                                        form=et_form)
            else:
                create_mangan_event_type(et_form.cg.data, et_form.et.data)
                flash('created Event Type ' +
                      et_form.et.data +
                      ' in Consumer Group ' +
                      et_form.cg.data)
                return redirect(url_for('mangan_index') +
                                "?c_group=" +
                                et_form.cg.data)
        elif request.method == 'GET':
            cg = request.args.get('c_group')
            et_form = ManganEventTypeForm(cg=cg)
            return check_and_render('mangan_create_et.html',
                                    form=et_form,
                                    cg=cg)
    else:
        return check_and_render('index.html')


@app.route('/mangan_offsets')
def mangan_offsets():
    """Investigate on Mangan Offsets."""
    if only_check():
        et = request.args.get('et')
        offsets = get_mangan_offsets(et)
        return check_and_render('mangan_offsets.html',
                                et=et,
                                offsets=offsets)
    else:
        return check_and_render('index.html')


@app.route('/mangan_offsets/set')
def mangan_set_offset():
    """Investigate on Mangan Offsets."""
    if only_check():
        key = request.args.get('key')
        value = request.args.get('value')
        set_mangan_offset(key, value)
        offsets = get_mangan_offsets(key[:-1])
        return check_and_render('mangan_offsets.html',
                                et=key[:-1],
                                offsets=offsets)
    else:
        return check_and_render('index.html')


# @app.route('/metrics')
# def metrics():
#     return json.dumps(uwsgi_metrics.view())


if (__name__ == '__main__'):
    app.run(debug=True, host='0.0.0.0')
