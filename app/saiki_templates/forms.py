from flask_wtf import Form
from wtforms.widgets import TextArea
from wtforms.fields import StringField
from wtforms.validators import DataRequired

from app.validations import validate_regx


class TemplateForm(Form):
    template_name = StringField('Template-Name',
                                validators=[DataRequired(),
                                            validate_regx])
    template_data = StringField('Template-Data', widget=TextArea(),
                                validators=[DataRequired()])
