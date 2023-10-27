import logging

import redis
from nameko.events import event_handler
from nameko.extensions import DependencyProvider
from nameko.rpc import rpc

from products import schemas

logger = logging.getLogger(__name__)

class RedisCache(DependencyProvider):

    def setup(self):
        redis_url = self.container.config.get('REDIS_URL', 'redis://localhost:6379')
        self.client = redis.StrictRedis.from_url(redis_url)

    def get_dependency(self, worker_ctx):
        return self.client


class ProductsService:
    name = 'products'
    storage = dependencies.Storage()
    redis_cache = RedisCache()
    product_schema = schemas.Product(strict=True)

    @rpc
    def get(self, product_id):
        cached_product = self.redis_cache.get(f'product:{product_id}')
        if cached_product:
            return self.product_schema.loads(cached_product)

        product = self.storage.get(product_id)
        if not product:
            return None  # or raise a custom error

        self.redis_cache.set(f'product:{product_id}', self.product_schema.dumps(product).data)

        return self.product_schema.dump(product).data

    @rpc
    def list(self):
        products = self.storage.list()
        return self.product_schema.dump(products, many=True).data

    @rpc
    def create(self, product_data):
        product = self.product_schema.load(product_data).data
        self.storage.create(product)

    @rpc
    def delete(self, product_id):
        if not self.storage.get(product_id):
            raise Exception("Product not found")
        self.storage.delete(product_id)
        self.redis_cache.delete(f'product:{product_id}')

    @event_handler('orders', 'order_created')
    def handle_order_created(self, payload):
        for product in payload['order']['order_details']:
            self.storage.decrement_stock(
                product['product_id'], product['quantity'])
            self.redis_cache.delete(f'product:{product_id}')
