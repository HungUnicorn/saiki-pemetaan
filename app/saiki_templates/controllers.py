from app.zookeeper import init_zk, get_namespace_saiki

# Import flask dependencies
from flask import Blueprint, flash, redirect, url_for, request

from app.auth import check_and_render, only_check
from kazoo.client import NoNodeError, NodeExistsError
import urllib
from app.settings.controllers import get_settings

from app.saiki_templates.forms import TemplateForm

from app.topics.controllers import validate_topic

namespace_saiki = get_namespace_saiki()

mod_saiki_templates = Blueprint('saiki_templates',
                                __name__,
                                url_prefix='/saiki_templates')


@mod_saiki_templates.route('/', methods=('GET', 'POST'))
def saiki_templates():
    """Docstring."""
    templates = get_saiki_templates()
    return check_and_render('saiki_templates/index.html',
                            display_settings=get_settings(),
                            templates=templates)


@mod_saiki_templates.route('/delete', methods=('GET', 'POST'))
def saiki_templates_delete():
    """Docstring."""
    if only_check():
        template = request.args.get('template')
        delete_template(template)
        flash('Deleted Template: ' + template)
        return redirect(url_for('saiki_templates.saiki_templates'))
    else:
        check_and_render('index.html', display_settings=get_settings())


@mod_saiki_templates.route('/edit', methods=('GET', 'POST'))
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
                return check_and_render('saiki_templates/edit.html',
                                        display_settings=get_settings(),
                                        form=template_form)
            else:
                update_template(template_form)
                flash('updated Config for Topic : ' +
                      template_form.template_name.data)
                return redirect(url_for('saiki_templates.saiki_templates'))
        elif request.method == 'GET':
            template = request.args.get('template')
            if template != '' and template is not None:
                template_data = get_saiki_template_single(template)
            else:
                template = ''
                template_data = '{}'
            template_form = TemplateForm(template_name=template,
                                         template_data=template_data)
            return check_and_render('saiki_templates/edit.html',
                                    display_settings=get_settings(),
                                    form=template_form,
                                    template_data=template_data)
    else:
        return check_and_render('index.html', display_settings=get_settings())


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
