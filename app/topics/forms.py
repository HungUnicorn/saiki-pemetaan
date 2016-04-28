from flask_wtf import Form
from wtforms import IntegerField, HiddenField, SelectField, FloatField, \
    SelectMultipleField, widgets
from wtforms.fields import StringField
from wtforms.validators import DataRequired
from app.validations import validate_regx


class TopicForm(Form):
    topic_name = StringField('Topic-Name', validators=[DataRequired(),
                                                       validate_regx])
    replication_factor = IntegerField('Replication Factor',
                                      validators=[DataRequired()])
    partition_count = IntegerField('Partition Count',
                                   validators=[DataRequired()])


class ConfigForm(Form):
    topic = HiddenField("Topic")
    retention_ms = IntegerField('retention_ms')
    max_message_bytes = IntegerField('max_message_bytes')
    options_cleanup_policy = ['delete', 'compact']
    cleanup_policy = SelectField('cleanup_policy',
                                 choices=[(f, f) for f in
                                          options_cleanup_policy])
    flush_messages = IntegerField('flush_messages')
    flush_ms = IntegerField('flush_ms')
    index_interval_bytes = IntegerField('index_interval_bytes')
    min_cleanable_dirty_ratio = FloatField('min_cleanable_dirty_ratio')
    min_insync_replicas = IntegerField('min_insync_replicas')
    retention_bytes = IntegerField('retention_bytes')
    segment_index_bytes = IntegerField('segment_index_bytes')
    segment_bytes = IntegerField('segment_bytes')
    segment_ms = IntegerField('segment_ms')
    segment_jitter_ms = IntegerField('segment_jitter_ms')
    delete_retention_ms = IntegerField('delete_retention_ms')


class MultiCheckboxField(SelectMultipleField):
    """
    A multiple-select, except displays a list of checkboxes.

    Iterating the field will produce subfields, allowing custom rendering of
    the enclosed checkbox fields.
    """
    widget = widgets.ListWidget(prefix_label=False)
    option_widget = widgets.CheckboxInput()
