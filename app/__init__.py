# Import flask and template operators
from flask import Flask, render_template, session, flash, redirect, url_for, \
    request
from flask_bootstrap import Bootstrap

from app.settings.controllers import get_settings
from app.auth import check_and_render

import json
import os
import logging
from flask_oauthlib.client import OAuth

# Import a module / component using its blueprint handler variable (mod_auth)
from app.brokers.controllers import mod_brokers as brokers_module
from app.settings.controllers import mod_settings as settings_module
from app.topics.controllers import mod_topics as topics_module
from app.topic_mapping.controllers import \
    mod_topic_mapping as topic_mapping_module
from app.mangan.controllers import mod_mangan as mangan_module
from app.saiki_templates.controllers import \
    mod_saiki_templates as saiki_templates_module

# Import SQLAlchemy
# from flask.ext.sqlalchemy import SQLAlchemy

# Define the WSGI application object
app = Flask(__name__)

# Configurations
app.config.from_object('config')

Bootstrap(app)

oauth = OAuth(app)

with open(os.path.join(os.getenv('CREDENTIALS_DIR',
                                 ''), 'client.json')) as fd:
    client_credentials = json.load(fd)

oauth_api_endpoint = os.getenv('ACCESS_TOKEN_URL', '')

if oauth_api_endpoint == '':
    logging.error("no OAuth Endpoint provided. Exiting ...")
    exit(1)

auth = oauth.remote_app(
    'auth',
    consumer_key=client_credentials['client_id'],
    consumer_secret=client_credentials['client_secret'],
    request_token_params={'scope': 'uid'},
    base_url=oauth_api_endpoint,
    request_token_url=None,
    access_token_method='POST',
    access_token_url=oauth_api_endpoint + '/oauth2/' +
                                          'access_token?realm=employees',
    authorize_url=oauth_api_endpoint + '/oauth2/'
                                       'authorize?realm=employees'
)


@app.errorhandler(404)
def not_found(error):
    return render_template('404.html', error=error), 404

# Register blueprint(s)
app.register_blueprint(brokers_module)
app.register_blueprint(settings_module)
app.register_blueprint(topics_module)
app.register_blueprint(topic_mapping_module)
app.register_blueprint(mangan_module)
app.register_blueprint(saiki_templates_module)


@app.route('/', methods=('GET', 'POST'))
def index():
    """Main Page."""
    return check_and_render('index.html',
                            display_settings=get_settings(),
                            force_render=True)


@app.template_filter('to_json')
def to_json(value):
    """To_Json function for flask."""
    return json.dumps(value, indent=4)


@app.route('/health')
def health():
    """Health Endpoint."""
    return 'OK'


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
