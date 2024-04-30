from pprint import pprint
from typing import Any

from kombu import Connection, Exchange, Queue


def callback(data: dict[str, Any], message: str):
    print("=====")
    pprint(data, indent=2)
    pprint(message, indent=2)


async def listen(user: str, password: str):
    with Connection(
        hostname="pulse.mozilla.org",
        port=5671,
        userid=user,
        password=password,
        ssl=True,
    ) as connection:
        exchange = Exchange(
            "exchange/taskcluster-queue/v1/task-completed", type="topic"
        )
        exchange(connection).declare(
            passive=True
        )  # raise an error if exchange doesn't exist

        queue = Queue(
            name=f"queue/{user}/fxci-etl",
            exchange=exchange,
            routing_key="#",
            durable=False,
            exclusive=False,
            auto_delete=True,
        )

        consumer = connection.Consumer(queue, auto_declare=False, callbacks=[callback])
        consumer.queues[0].queue_declare()
        consumer.queues[0].queue_bind()

        with consumer:
            while True:
                connection.drain_events(timeout=None)
