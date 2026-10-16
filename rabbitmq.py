

import json
import aio_pika
from aio_pika.abc import AbstractChannel, AbstractExchange, AbstractQueue


NOTIFICATIONS_QUEUE = "notifications.order_created"
ORDER_EXCHANGE = "order"
ORDER_ROUTING_KEY = "order.created"


async def connect_rabbitmq():
    return await aio_pika.connect_robust("amqp://guest:guest@localhost:5672/")


async def declare_orders_exchange(channel: AbstractChannel):
    return await channel.declare_exchange(ORDER_EXCHANGE)


async def declare_notifications_queue(
    channel: AbstractChannel,
    exchange: AbstractExchange
):
    exchange.publish
    queue: AbstractQueue = await channel.declare_queue(NOTIFICATIONS_QUEUE)
    await queue.bind(exchange, routing_key=ORDER_ROUTING_KEY)
    return queue


async def publish_json(exchange: AbstractExchange, routing_key: str, data: dict):
    message = aio_pika.Message(json.dumps(data).encode())
    await exchange.publish(message, routing_key)
