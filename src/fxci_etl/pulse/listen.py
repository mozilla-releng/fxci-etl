from kombu import Connection, Exchange, Queue

from fxci_etl.config import Config
from fxci_etl.pulse.handlers.base import handlers


async def listen(config: Config, name: str):
    pulse = config.pulse
    qconf = pulse.queues[name]

    with Connection(
        hostname=pulse.host,
        port=pulse.port,
        userid=pulse.user,
        password=pulse.password,
        ssl=True,
    ) as connection:
        exchange = Exchange(qconf.exchange, type="topic")
        exchange(connection).declare(
            passive=True
        )  # raise an error if exchange doesn't exist

        queue = Queue(
            name=f"queue/{pulse.user}/{name}",
            exchange=exchange,
            routing_key=qconf.routing_key,
            durable=qconf.durable,
            exclusive=False,
            auto_delete=qconf.auto_delete,
        )

        callbacks = [
            cls(config)
            for name, cls in handlers.items()
            if config.etl.handlers is None or name in config.etl.handlers
        ]
        consumer = connection.Consumer(queue, auto_declare=False, callbacks=callbacks)
        consumer.queues[0].queue_declare()
        consumer.queues[0].queue_bind()

        with consumer:
            while True:
                try:
                    connection.drain_events(timeout=None)
                except TimeoutError:
                    pass
