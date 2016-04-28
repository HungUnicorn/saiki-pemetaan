from flask_wtf import Form
from wtforms import BooleanField
from wtforms.fields import StringField
from wtforms.validators import DataRequired
from app.validations import validate_regx


class MappingForm(Form):
    content_type = StringField('Content-Type',
                               validators=[DataRequired(),
                                           validate_regx])

    topic = StringField('Topic', validators=[DataRequired(),
                                             validate_regx])
    active = BooleanField('Active Mapping?',
                          description='Should this be the active mapping for \
                          this Content-Type?')
