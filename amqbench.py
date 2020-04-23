#!/usr/bin/env python

import argparse
import os
import socket

from six.moves import range
from time import time

from kombu import BrokerConnection, Exchange, Queue, Producer, Consumer
from kombu.utils import json

str_to_bool = {
    "yes": True,
    "no": False,
    "1": True,
    "0": False,
    "true": True,
    "false": False,
}

MESSAGE = {"task": "foo.bar", "args": (1, 2), "kwargs": {"callback": "foo"}}
DURABLE = str_to_bool[os.environ.get("DURABLE", "yes").lower()]
DM = int(os.environ.get("DM", 2))  # delivery-mode


def serialize(msg):
    return json.dumps(msg)


def deserialize(data):
    return json.loads(data)


def print_avg(name, action, n, d):
    if n > 0:
        avg = d / n * 1000
    else:
        avg = "n/a "

    print("{}.{} [n={}] {} (avg={}ms)".format(name, action, n, d, avg))


class _Consumer(Consumer):
    def __init__(self, *args, **kwargs):
        self.i = 0
        super(_Consumer, self).__init__(*args, **kwargs)

    def _receive_callback(self, raw_message):
        self.i += 1
        if not self.i % 10000:
            print(self.i)


def _declare(chan, name, durable=DURABLE):
    chan.exchange_declare(
        exchange=name, type="direct", durable=durable, auto_delete=True
    )
    chan.queue_declare(queue=name, durable=durable, auto_delete=True)
    chan.queue_bind(queue=name, exchange=name, routing_key=name)

    return chan


def _kpublish(n, name, conn, durable=DURABLE, delivery_mode=DM, auto_declare_exchange=True):
    _cleanup(conn.clone, name)
    channel = conn.channel()
    exchange = Exchange(
        name, type="direct", auto_declare=False, durable=durable, auto_delete=True
    )
    Queue(name, exchange, name, durable=durable, auto_delete=True)(channel).declare()

    print("** Kombu Producer exchange auto_declare = {}".format(auto_declare_exchange))
    if auto_declare_exchange:
        producer = Producer(channel, exchange, serializer="json")
    else:
        # disable auto_declare. See https://github.com/celery/kombu/issues/651
        producer = Producer(channel, exchange, serializer="json", auto_declare=False)
        producer.declare()

    message = MESSAGE

    start = time()
    for i in range(n):
        if not i % 10000:
            print_avg(name, "publish", i, time() - start)
        producer.publish(message, routing_key=name, delivery_mode=delivery_mode)

    print_avg(name, "publish", n, time() - start)


def _kconsume(n, name, conn, durable=DURABLE):
    channel = conn.channel()
    exchange = Exchange(name, type="direct", durable=durable, auto_delete=True)
    queue = Queue(name, exchange, name, durable=durable, auto_delete=True)
    consumer = Consumer(channel, queue, accept=["json", "pickle", "msgpack", "yaml"])
    conn.connect()

    i = [0]

    def callback(message_data, message=None):
        i[0] += 1

        if not i[0] % 10000:
            print_avg(name, "consume", i[0], time() - start)

        if i[0] == 1:
            body = message.body
            if isinstance(body, memoryview):
                body = body.tobytes()

    consumer.register_callback(callback)
    consumer.consume(no_ack=True)

    start = time()
    while i[0] < n:
        try:
            conn.drain_events()
        except socket.timeout:
            pass

    print_avg(name, "consume", n, time() - start)


def _publish(n, Connection, Message, name):
    props = {"delivery_mode": DM}
    kwargs = {"properties": props}

    _cleanup(Connection, name)
    conn = Connection()

    if name == "amqp":
        kwargs = props
        conn.connect()

    chan = conn.channel()
    chan = _declare(chan, name)

    def _new_message():
        return Message(serialize(MESSAGE), **kwargs)

    _Message = _new_message

    def _new_librabbitmq_message():
        return (serialize(MESSAGE), props)

    if name == "librabbitmq":
        _Message = _new_librabbitmq_message

    start = time()
    for i in range(n):
        if not i % 10000:
            print_avg(name, "publish", i, time() - start)

        message = _Message()
        chan.basic_publish(message, exchange=name, routing_key=name)

    print_avg(name, "publish", n, time() - start)


def _cleanup(Connection, name, _chan=None):
    pass


def _consume(n, Connection, name):
    conn = Connection()

    if name == "amqp":
        conn.connect()

    chan = conn.channel()
    chan = _declare(chan, name)

    i = [0]

    def callback(message):
        assert len(message.body) > 10
        i[0] += 1
        if not i[0] % 10000:
            print_avg(name, "consume", i[0], time() - start)
        if i[0] == 1:
            body = message.body
            if isinstance(body, memoryview):
                body = body.tobytes()
        deserialize(message.body)
        # chan.basic_ack(message.delivery_info["delivery_tag"])

    chan.basic_consume(queue=name, no_ack=True, callback=callback)

    if True or hasattr(conn, "drain_events"):
        wait = conn.drain_events
    else:
        wait = chan.wait

    start = time()
    while i[0] < n:
        wait()

    print_avg(name, "consume", n, time() - start)

    chan.close()
    conn.close()


class amqp(object):
    def publish(self, n=52000):
        from amqp import Connection, Message

        _publish(n, Connection, Message, "amqp")

    def consume(self, n=52000):
        from amqp import Connection

        _consume(n, Connection, "amqp")


class librabbitmq(object):
    def publish(self, n=52000):
        from librabbitmq import Connection, Message

        _publish(n, Connection, Message, "librabbitmq")

    def consume(self, n=52000):
        from librabbitmq import Connection

        _consume(n, Connection, "librabbitmq")


class KombuBench(object):
    transport = None
    auto_declare_exchange = True

    def publish(self, n=52000):
        _kpublish(
            n, self.__class__.__name__, BrokerConnection(transport=self.transport), auto_declare_exchange=self.auto_declare_exchange
        )

    def consume(self, n=52000):
        _kconsume(
            n, self.__class__.__name__, BrokerConnection(transport=self.transport)
        )


class klibrabbitmq(KombuBench):
    transport = "librabbitmq"


class kamqp(KombuBench):
    transport = "amqp"


class kmemory(KombuBench):
    transport = "memory"

    def bench(self, n=52000):
        self.publish(n)
        self.consume(n)


types = {
    "amqp": ("celery/py-amqp", amqp()),
    "librabbitmq": ("librabbitmq-fork", librabbitmq()),
    "klibrabbitmq": ("kombu with librabbitmq", klibrabbitmq()),
    "kamqp": ("kombu with py-amqp", kamqp()),
    "kmemory": ("kombu with memory transport", kmemory()),
}


def get_parser():
    parser = argparse.ArgumentParser(description="AMQP benchmark tool")

    parser.argument_default = argparse.SUPPRESS
    parser.add_argument(
        "-t",
        "--type",
        help="Test type",
        choices=["amqp", "librabbitmq", "kamqp", "klibrabbitmq", "kmemory"],
        required=True,
    )

    parser.add_argument("-n", default=52000, help="Number of messages")
    parser.add_argument(
        "-d", "--disable-exchange-auto-declare",
        action="store_true",
        default=False,
        help="Disable lazy exchange declare in kombu Producer"
    )

    return parser


def main(args):
    descr, runner = types[args.type]

    print("Test type: {}\n".format(descr))

    if args.disable_exchange_auto_declare:
        runner.auto_declare_exchange = False

    runner.publish(args.n)
    print("\n")
    runner.consume(args.n)


if __name__ == "__main__":
    main(get_parser().parse_args())
