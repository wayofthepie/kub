#!/usr/bin/env python

import pika
from kubernetes import client, config
from kub.log import log
from kub.job import QueueConsumer, JobExecutor
import time

if __name__ == '__main__':
    config.load_kube_config()
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
    log("Listening for Job requests ...")
    QueueConsumer(connection, "job_queue", 5, JobExecutor(client).callback).listen_and_consume()
