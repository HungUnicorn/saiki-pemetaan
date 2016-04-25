import urllib

from controllers.topic import validate_topic
from kazoo.client import NoNodeError, NodeExistsError
from zookeeper import init_zk, get_namespace_saiki

namespace_saiki = get_namespace_saiki()


def get_saiki_templates():
    """Docstring."""
    zk = init_zk(namespace_saiki)

    try:
        return zk.get_children('/templates')
    except NoNodeError:
        return []


def get_saiki_template_single(template):
    """Docstring."""
    zk = init_zk(namespace_saiki)

    try:
        data, stat = zk.get('/templates/' + template)
        return urllib.parse.unquote(data.decode('utf-8'))
    except NoNodeError:
        return [{'error': 'ZK: NoNodeError'}]


def update_template(template_form):
    """Docstring."""
    zk = init_zk(namespace_saiki)
    template = template_form.template_name.data
    template_data = template_form.template_data.data.encode()
    if validate_topic(template):
        try:
            zk.create('/templates/' + template, template_data)
            return True
        except NodeExistsError:
            zk.set('/templates/' + template, template_data)
            return True


def delete_template(template):
    """Docstring."""
    zk = init_zk(namespace_saiki)
    zk.delete('/templates/' + template)
