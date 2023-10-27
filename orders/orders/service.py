import redis
from nameko.events import EventDispatcher
from nameko.extensions import DependencyProvider
from nameko.rpc import rpc
from nameko_sqlalchemy import DatabaseSession
from orders.exceptions import NotFound
from orders.models import DeclarativeBase, Order, OrderDetail
from orders.schemas import OrderSchema

class RedisCache(DependencyProvider):

    def setup(self):
        redis_url = self.container.config.get('REDIS_URL', 'redis://localhost:6379')
        self.client = redis.StrictRedis.from_url(redis_url)

    def get_dependency(self, worker_ctx):
        return self.client


class OrdersService:
    name = 'orders'

    db = DatabaseSession(DeclarativeBase)
    redis_cache = RedisCache()
    event_dispatcher = EventDispatcher()

    @rpc
    def get_order(self, order_id):
        cached_order = self.redis_cache.get(f'order:{order_id}')
        if cached_order:
            return OrderSchema().loads(cached_order)

        order = self.db.query(Order).get(order_id)
        if not order:
            raise NotFound(f'Order with id {order_id} not found')

        self.redis_cache.set(f'order:{order_id}', OrderSchema().dumps(order).data)

        return OrderSchema().dump(order).data

    @rpc
    def list_orders(self):
        orders = self.db.query(Order).all()
        return OrderSchema(many=True).dump(orders).data

    @rpc
    def create_order(self, order_details):
        order = Order(order_details=[OrderDetail(**detail) for detail in order_details])
        self.db.add(order)
        self.db.commit()

        order_data = OrderSchema().dump(order).data
        self.event_dispatcher('order_created', {'order': order_data})

        return order_data

    @rpc
    def update_order(self, order):
        existing_order = self.db.query(Order).get(order['id'])
        order_details = {detail['id']: detail for detail in order['order_details']}

        for order_detail in existing_order.order_details:
            detail_data = order_details[order_detail.id]
            order_detail.price = detail_data['price']
            order_detail.quantity = detail_data['quantity']

        self.db.commit()

        self.redis_cache.delete(f'order:{order["id"]}')

        return OrderSchema().dump(existing_order).data

    @rpc
    def delete_order(self, order_id):
        order = self.db.query(Order).get(order_id)
        if order:
            self.db.delete(order)
            self.db.commit()
            self.redis_cache.delete(f'order:{order_id}')
        else:
            raise NotFound(f'Order with id {order_id} not found')
