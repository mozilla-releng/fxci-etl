from kombu import Connection, Exchange, Queue

from fxci_etl.config import Config
from fxci_etl.pulse.handlers.base import handlers


async def listen(config: Config):
    pulse = config.pulse
    with Connection(
        hostname=pulse.host,
        port=pulse.port,
        userid=pulse.user,
        password=pulse.password,
        ssl=True,
    ) as connection:
        exchange = Exchange(
            "exchange/taskcluster-queue/v1/task-completed", type="topic"
        )
        exchange(connection).declare(
            passive=True
        )  # raise an error if exchange doesn't exist

        queue = Queue(
            name=f"queue/{pulse.user}/{pulse.queue}",
            exchange=exchange,
            routing_key="#",
            durable=pulse.durable,
            exclusive=False,
            auto_delete=pulse.auto_delete,
        )

        callbacks = [
            cls(config)
            for name, cls in handlers.items()
            if config.etl.handlers is None or name in config.etl.handlers
        ]
        print(callbacks)
        consumer = connection.Consumer(queue, auto_declare=False, callbacks=callbacks)
        consumer.queues[0].queue_declare()
        consumer.queues[0].queue_bind()

        with consumer:
            while True:
                connection.drain_events(timeout=None)
