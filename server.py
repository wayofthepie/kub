#!/usr/bin/env python

import pika
from kubernetes import client, config
from kub.log import log
from kub.job import QueueConsumer, JobExecutor, JobSpecCreator
import time

if __name__ == '__main__':
    config.load_kube_config()
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
    log("Listening for Job requests ...")
    job_creator = JobSpecCreator(client)
    QueueConsumer(connection, "job_queue", 5, JobExecutor(client, job_creator).callback).listen_and_consume()
