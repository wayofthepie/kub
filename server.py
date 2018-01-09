#!/usr/bin/env python
import json
import threading
import uuid

import pika
from kubernetes import client, config

from model import job


def callback_with_batch(batch):
    def callback(ch, method, properties, body):
        def async():
            msg = json.loads(body.decode("utf8"))
            # FIXME: Build job from body
            executable = job("test-" + str(uuid.uuid4()), "alpine", ["echo", "test"])
            resp = batch.create_namespaced_job("default", executable)
            # FIXME: Do something meaningful with response ...
            print("[x] %r " % resp)
            ch.basic_ack(delivery_tag=method.delivery_tag)

        t = threading.Thread(target=async)
        t.start()

    return callback


if __name__ == '__main__':
    config.load_kube_config()
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='task_queue', durable=True)

    channel.basic_qos(prefetch_count=5)
    channel.basic_consume(callback_with_batch(client.BatchV1Api()), queue='task_queue')

    channel.start_consuming()
