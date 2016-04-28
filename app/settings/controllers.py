from app.zookeeper import init_zk, get_namespace_pemetaan
import json
from kazoo.client import NoNodeError
from flask import Blueprint, request

from app.auth import check_and_render, only_check

namespace_pemetaan = get_namespace_pemetaan()

mod_settings = Blueprint('settings', __name__, url_prefix='/settings')


@mod_settings.route('/')
def pemetaan_settings():
    """Creating Settings Overview Page."""
    if only_check():
        setting = request.args.get('setting')
        value = request.args.get('value')
        if setting is not None and value is not None:
            from distutils.util import strtobool
            update_settings(setting, strtobool(value))
            return check_and_render('settings/index.html',
                                    display_settings=get_settings(),
                                    settings=get_settings())
        else:
            return check_and_render('settings/index.html',
                                    display_settings=get_settings(),
                                    settings=get_settings())
    else:
        return check_and_render('index.html', display_settings=get_settings())


def update_settings(key_to_update, value_to_update):
    """Set Pemetaan Settings in ZK."""
    settings = get_settings()
    settings[key_to_update] = value_to_update
    zk = init_zk(namespace_pemetaan)
    zk.set('/settings', json.dumps(settings).encode('utf-8'))


def get_settings():
    """Get Pemetaan Settings from ZK."""
    zk = init_zk(namespace_pemetaan)

    try:
        data, stat = zk.get('/settings')
        return json.loads(data.decode('utf-8'))
    except NoNodeError:
        return []
