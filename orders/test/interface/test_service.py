import pytest
from nameko.exceptions import RemoteError
from orders.models import Order, OrderDetail
from orders.schemas import OrderSchema, OrderDetailSchema
import redis

@pytest.fixture
def redis_client():
    client = redis.StrictRedis(host='localhost', port=6379, db=0)
    yield client
    client.flushdb()


@pytest.fixture
def order(db_session):
    order = Order()
    db_session.add(order)
    db_session.commit()
    return order


@pytest.fixture
def order_details(db_session, order):
    details = [
        OrderDetail(order=order, product_id="the_odyssey", price=99.51, quantity=1),
        OrderDetail(order=order, product_id="the_enigma", price=30.99, quantity=8)
    ]
    db_session.add_all(details)
    db_session.commit()
    return details


def test_get_order(orders_rpc, order, redis_client):
    response = orders_rpc.get_order(1)
    assert response['id'] == order.id

    cached_order = redis_client.get(str(order.id))
    assert cached_order is not None


@pytest.mark.usefixtures('db_session')
def test_will_raise_when_order_not_found(orders_rpc):
    with pytest.raises(RemoteError) as err:
        orders_rpc.get_order(1)
    assert err.value.value == 'Order with id 1 not found'


@pytest.mark.usefixtures('db_session')
def test_can_create_order(orders_service, orders_rpc, redis_client):
    order_details = [
        {
            'product_id': "the_odyssey",
            'price': 99.99,
            'quantity': 1
        },
        {
            'product_id': "the_enigma",
            'price': 5.99,
            'quantity': 8
        }
    ]
    new_order = orders_rpc.create_order(
        OrderDetailSchema(many=True).dump(order_details).data
    )
    assert new_order['id'] > 0
    assert len(new_order['order_details']) == len(order_details)
    assert redis_client.get(str(new_order['id'])) is None


@pytest.mark.usefixtures('db_session', 'order_details')
def test_can_update_order(orders_rpc, order, redis_client):
    order_payload = OrderSchema().dump(order).data
    for order_detail in order_payload['order_details']:
        order_detail['quantity'] += 1

    updated_order = orders_rpc.update_order(order_payload)

    assert updated_order['order_details'] == order_payload['order_details']
    assert redis_client.get(str(order.id)) is None


def test_can_delete_order(orders_rpc, order, db_session, redis_client):
    orders_rpc.delete_order(order.id)

    assert not db_session.query(Order).filter_by(id=order.id).count()
    assert redis_client.get(str(order.id)) is None
