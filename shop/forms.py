from wtforms import Form, IntegerField, validators, StringField


class OrderCreateForm(Form):
    user_id = IntegerField('User id', [validators.DataRequired()])


class OrderChangeStatusForm(Form):
    aggregated_id = StringField('Order id', [validators.DataRequired()])
    new_status = StringField('New order status', [validators.DataRequired()])
