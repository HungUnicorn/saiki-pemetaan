from flask_wtf import Form
from wtforms import TextField,\
    BooleanField, IntegerField, HiddenField, SelectField,\
    SelectMultipleField, widgets
from wtforms.widgets import TextArea
from wtforms.fields import StringField
from wtforms.validators import Required


class MappingForm(Form):
    content_type = TextField('Content-Type', validators=[Required()])
    topic = TextField('Topic', validators=[Required()])
    active = BooleanField('Active Mapping?',
                          description='Should this be the active mapping for \
                          this Content-Type?')


class ConsumerGroupForm(Form):
    consumer_group = TextField('Consumer Group', validators=[Required()])


class ManganEventTypeForm(Form):
    et = TextField('Event Type', validators=[Required()])
    cg = HiddenField('Consumer Group', validators=[Required()])


class TopicForm(Form):
    topic_name = TextField('Topic-Name',
                           validators=[Required()])
    replication_factor = IntegerField('Replication Factor',
                                      validators=[Required()])
    partition_count = IntegerField('Partition Count',
                                   validators=[Required()])


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
    min_cleanable_dirty_ratio = IntegerField('min_cleanable_dirty_ratio')
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


class TemplateForm(Form):
    template_name = TextField('Template-Name',
                              validators=[Required()])
    template_data = StringField('Template-Data', widget=TextArea(),
                                validators=[Required()])
