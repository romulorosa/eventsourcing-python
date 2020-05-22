from flask import Flask, request, render_template
from flask_socketio import SocketIO, join_room

from db_connectors.databases import database_connect
from shop.aggregators import AggregatesRepository, Order
from shop.event_store import RethinkDbEventStore
from shop.events import OrderStatusChanged, OrderCreated
from shop.forms import OrderCreateForm, OrderChangeStatusForm

app = Flask(__name__)
# app.config.from_object()
socketio = SocketIO(app)
database_obj, database_connection = database_connect()


@app.route('/command/order/create', methods=['POST', 'GET'])
def create_order():
    form = OrderCreateForm(request.form)
    if request.method == 'POST':
        event_store = RethinkDbEventStore(database_obj, database_connection)
        repository = AggregatesRepository(event_store)
        order = Order.create(form.user_id.data)
        repository.save(order)
        return 'Order %s created' % order.uuid, 200

    return render_template('order_create.html', form=form)


@app.route('/command/order/status/update', methods=['GET', 'POST'])
def update_order_status():
    form = OrderChangeStatusForm(request.form)

    if request.method == 'POST':
        event_store = RethinkDbEventStore(database_obj, database_connection)
        repository = AggregatesRepository(event_store)
        order = repository.get(Order, form.aggregated_id.data)
        status_event = OrderStatusChanged(form.new_status.data)
        order.apply(status_event)
        repository.save(order)
        socketio.send(
            '%s %s' % (status_event.name, status_event.new_status), room=form.aggregated_id.data
        )
        return 'Status has been changed to %s' % form.new_status.data, 200

    return render_template('order_update_status.html', form=form)


@app.route('/query/order/<aggregated_id>', methods=['GET'])
def get_order(aggregated_id):
    event_store = RethinkDbEventStore(database_obj, database_connection)
    repository = AggregatesRepository(event_store)
    order = repository.get(Order, aggregated_id)
    events = order.event_stream.events
    events_info = [
        '%s %s' % (event.name, event.created_at)
        if isinstance(event, OrderCreated)
        else '{} {} {}'.format(event.name, event.new_status, event.created_at)
        for event in events
    ]

    return render_template(
        'order_detail.html', aggregated_id=aggregated_id, events=events_info, status=order.status
    )


@app.route('/query/order/subscribe')
def subscribe():
    return render_template('order_events_stream.html')


@socketio.on('connected')
def connected():
    print('%s connected' % request.sid)


@socketio.on('message')
def order_event_subscribe(aggregated_id):
    join_room(aggregated_id)
    print('%s has joined the room %s' % (request.sid, aggregated_id))

    event_store = RethinkDbEventStore(database_obj, database_connection)
    repository = AggregatesRepository(event_store)
    order = repository.get(Order, aggregated_id)
    events = order.event_stream.events

    events_info = [
        '%s' % event.name
        if isinstance(event, OrderCreated)
        else '{} {}'.format(event.name, event.new_status)
        for event in events
    ]

    for event in events_info:
        socketio.send(event, room=request.sid)


if __name__ == '__main__':
    socketio.run(app)
