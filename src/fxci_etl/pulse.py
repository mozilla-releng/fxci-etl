from pprint import pprint
from typing import Any

from kombu import Connection, Exchange, Queue

from fxci_etl.config import PulseConfig


def callback(data: dict[str, Any], message: str):
    print("=====")
    pprint(data, indent=2)
    pprint(message, indent=2)


async def listen(config: PulseConfig):
    with Connection(
        hostname=config.host,
        port=config.port,
        userid=config.user,
        password=config.password,
        ssl=True,
    ) as connection:
        exchange = Exchange(
            "exchange/taskcluster-queue/v1/task-completed", type="topic"
        )
        exchange(connection).declare(
            passive=True
        )  # raise an error if exchange doesn't exist

        queue = Queue(
            name=f"queue/{config.user}/{config.queue}",
            exchange=exchange,
            routing_key="#",
            durable=config.durable,
            exclusive=False,
            auto_delete=config.auto_delete,
        )

        consumer = connection.Consumer(queue, auto_declare=False, callbacks=[callback])
        consumer.queues[0].queue_declare()
        consumer.queues[0].queue_bind()

        with consumer:
            while True:
                connection.drain_events(timeout=None)
