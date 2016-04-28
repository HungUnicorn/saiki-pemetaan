from flask import flash
from wtforms.validators import ValidationError
import re


def validate_regx(Form, field):
    pattern = r'^\w+$'
    match = re.match(pattern, field.data)
    if match is None:
        flash('"{}" is not valid. Please do not use whitespace, '
              'backslash and slash!'.format(field.data), 'critical')
        raise ValidationError('Not a valid name')
