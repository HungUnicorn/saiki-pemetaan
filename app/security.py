import json
import logging
import os

import requests
from flask import flash, session, render_template, redirect
from flask_oauthlib.client import OAuth


def get_auth_oauth_token():
    """Docstring."""
    return session.get('auth_token')


def get_auth(app):
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

    return auth


def check_team(user, team=os.getenv('TEAMCHECK_ID', ''), ):
    """
    Check if the logged in user is in the correct team, if supplied.

    in case one parameter is not supplied, the check will always return true.
    """
    token = get_auth_oauth_token()
    if team != '' and os.getenv('TEAMCHECK_API', '') != '':
        logging.debug("checking with teams api ...")
        logging.debug(token)
        url = os.getenv('TEAMCHECK_API', '') + team
        headers = {'Authorization': 'Bearer ' + token[0]}
        r = requests.get(url, headers=headers)
        member_list = json.loads(r.text)['member']
        if user in member_list:
            logging.debug("valid ...")
            return True
        else:
            logging.error("valid user but not in the correct team ...")
            return False
    else:
        return True


def validate_access_token():
    """Docstring."""
    global app_props
    oauth_tokeninfo_endpoint = os.getenv('TOKENINFO_URL', '')

    if 'auth_token' in session:
        response = requests.get("%s?access_token=%s" % (
                                oauth_tokeninfo_endpoint,
                                session['auth_token'][0]), verify=False)
        response.close()
        resp_json = response.json()
        if 'error' in resp_json:
            flash('Your token is not valid! (Expired?) Please login again! \
                   Error message: ' +
                  json.dumps(resp_json), 'critical')
            # delete all remaining token data
            session.pop('auth_token', None)
            return False, response.json()
        elif check_team(resp_json["uid"]) is False:
            flash('You are not in the Saiki Team, please apply for the ' +
                  'correct Roles! Logged out!', 'critical')
            # Should not be needed, auth token is not populated
            session.pop('auth_token', None)
            return False, response.json()
        else:
            return True, response.json()
    else:
        return False, None


def only_check():
    """Check if Oauth Login is valid."""
    isloggedin, token_info = validate_access_token()
    if isloggedin:
        return True
    else:
        return False


def check_and_render(template, force_render=False, **kwargs):
    """Check if Oauth Login is valid and render supplied page."""
    isloggedin, token_info = validate_access_token()
    if isloggedin:
        return render_template(template,
                               access_token=token_info,
                               display_settings=get_settings(),
                               **kwargs)
    elif (force_render is True):
        return render_template(template)
    else:
        flash('You need to be logged in to do that!', 'critical')
        return redirect(url_for('index'))