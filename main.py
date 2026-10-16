from contextlib import asynccontextmanager
from uuid import uuid4

from fastapi import FastAPI, Request
from pydantic import BaseModel

from rabbitmq import ORDER_ROUTING_KEY, connect_rabbitmq, declare_notifications_queue, declare_orders_exchange, publish_json


class Product(BaseModel):
    name: str
    price: int


class Order(BaseModel):
    user_id: int
    products: list[Product]


@asynccontextmanager
async def lifespan(_: FastAPI):
    connection = await connect_rabbitmq()
    channel = await connection.channel()
    orders_exchange = await declare_orders_exchange(channel)

    app.state.orders_exchange = orders_exchange
    
    yield
    await connection.close()


app = FastAPI(lifespan=lifespan)


@app.post("/orders")
async def create_order(payload: Order, request: Request):
    # Представим, что тут сохранение в БД

    event_data = {
        "event_id": str(uuid4()),
        "event": "order.created",
        "user_id": payload.user_id,
        "products": [product.model_dump() for product in payload.products]
    }

    await publish_json(
        exchange=request.app.state.orders_exchange,
        routing_key=ORDER_ROUTING_KEY,
        data=event_data
    )

    return "success"


