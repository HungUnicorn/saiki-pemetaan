from flask import render_template, session, flash, redirect, url_for
import json
import requests
import os
import logging

oauth_tokeninfo_endpoint = os.getenv('TOKENINFO_URL', '')


def only_check():
    """Check if Oauth Login is valid."""
    isloggedin, token_info = validate_access_token()
    if isloggedin:
        return True
    else:
        return False


def check_and_render(template, display_settings, force_render=False, **kwargs):
    """Check if Oauth Login is valid and render supplied page."""
    isloggedin, token_info = validate_access_token()
    if isloggedin:
        return render_template(template,
                               access_token=token_info,
                               display_settings=display_settings,
                               **kwargs)
    elif (force_render is True):
        return render_template(template)
    else:
        flash('You need to be logged in to do that!', 'critical')
        return redirect(url_for('index'))


def validate_access_token():
    """Docstring."""
    global app_props
    global oauth_tokeninfo_endpoint
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
        elif (check_team(resp_json["uid"]) is False):
            flash('You are not in the Saiki Team, please apply for the ' +
                  'correct Roles! Logged out!', 'critical')
            # Should not be needed, auth token is not populated
            session.pop('auth_token', None)
            return False, response.json()
        else:
            return True, response.json()
    else:
        return False, None


def check_team(user, team=os.getenv('TEAMCHECK_ID', ''), ):
    """
    Check if the logged in user is in the correct team, if supplied.
    in case one parameter is not supplied, the check will always return true.
    """
    token = get_auth_oauth_token()
    if team != '' and os.getenv('TEAMCHECK_API', '') != '':
        for team_single in team.split(','):
            logging.debug("checking with teams api for team " + team_single)
            logging.debug(token)
            url = os.getenv('TEAMCHECK_API', '') + team_single
            headers = {'Authorization': 'Bearer ' + token[0]}
            r = requests.get(url, headers=headers)
            member_list = json.loads(r.text)['member']
            if user in member_list:
                logging.debug("valid ...")
                return True
        logging.error("valid user but not in the correct team ...")
        return False
    else:
        return True


# @auth.tokengetter
def get_auth_oauth_token():
    """Docstring."""
    return session.get('auth_token')
