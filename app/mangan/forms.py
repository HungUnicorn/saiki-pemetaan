from flask_wtf import Form
from wtforms import StringField, HiddenField
from wtforms.validators import DataRequired


class ConsumerGroupForm(Form):
    consumer_group = StringField('Consumer Group', validators=[DataRequired()])


class ManganEventTypeForm(Form):
    et = StringField('Event Type', validators=[DataRequired()])
    cg = HiddenField('Consumer Group', validators=[DataRequired()])
