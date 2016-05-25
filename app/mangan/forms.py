from flask_wtf import Form
from wtforms import StringField, HiddenField, IntegerField
from wtforms.validators import DataRequired


class ConsumerGroupForm(Form):
    consumer_group = StringField('Consumer Group',
                                 validators=[DataRequired()])


class ManganEventTypeForm(Form):
    et = StringField('Event Type', validators=[DataRequired()])
    et_regex = StringField('Event Type Regex Pattern',
                           validators=[DataRequired()])
    cg = HiddenField('Consumer Group', validators=[DataRequired()])
    nakadi_endpoint = StringField('Nakadi Endpoint',
                                  validators=[DataRequired()])
    batch_size = IntegerField('Batch Size of Chunks sent to Lawang',
                              validators=[DataRequired()])
