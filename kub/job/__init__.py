import json
import threading
import uuid
from kub.log import log
from kubernetes import watch


class QueueConsumer:
    def __init__(self, connection, queue_name, prefetch_count, callback):
        self.__channel = self.__create_channel(connection, queue_name, prefetch_count,
                                               callback)

    def listen_and_consume(self):
        self.__channel.start_consuming()

    @staticmethod
    def __create_channel(connection, queue_name, prefetch_count, callback):
        channel = connection.channel()
        channel.queue_declare(queue=queue_name, durable=True)
        channel.basic_qos(prefetch_count=prefetch_count)
        channel.basic_consume(callback, queue=queue_name)
        return channel


class JobSpecCreator:
    def __init__(self, kube_client):
        self.__api_version = "batch/v1"
        self.__kind = "Job"
        self.__kube_client = kube_client

    def create(self, name, image, command, args, metadata_labels):
        data = self.__kube_client.V1Job()
        data.api_version = self.__api_version
        data.kind = self.__kind
        data.metadata = self.__meta(name, metadata_labels)
        data.spec = self.__spec(name, image, command, args)
        data.spec.backoffLimit = 1
        return data

    def __meta(self, name, labels):
        meta = self.__kube_client.V1ObjectMeta()
        meta.name = name
        meta.labels = labels

        return meta

    def __spec(self, name, image, command, args):
        template = self.__kube_client.V1PodTemplateSpec()
        template.metadata = self.__kube_client.V1ObjectMeta(name=name)
        template.spec = self.__kube_client.V1PodSpec(containers=[
            self.__kube_client.V1Container(name=name, image=image, command=command, args=args)
        ], restart_policy="Never")

        return self.__kube_client.V1JobSpec(template=template)


class JobExecutor:
    def __init__(self, kube_client):
        self.__kube_client = kube_client
        self.__job_creator = JobSpecCreator(kube_client)
        self.__batch = kube_client.BatchV1Api()

    def callback(self, ch, method, properties, body):
        def execute_job():
            msg = deserialize_to_json(body)
            log("Received message : {}".format(msg))

            name = "{}-{}".format(msg["name"], str(uuid.uuid4()))
            image = msg["image"]
            command = msg["command"]
            args = msg["args"]

            log("Starting Job {}".format(name))
            resp = self.__batch.create_namespaced_job(namespace="default",
                                                      body=self.__job_creator.create(name, image,
                                                                                     command, args, {}))

            # FIXME : Check response, do something on error for both above and below calls
            self.__wait_for_job_completion(job_name=name)

            log("Acking ...")
            ch.basic_ack(delivery_tag=method.delivery_tag)

        t = threading.Thread(target=execute_job)
        t.start()

    def __wait_for_job_completion(self, job_name):
        w = watch.Watch()
        kwargs = {"field_selector": "metadata.name={}".format(job_name), "_request_timeout": 600}
        for event in w.stream(self.__batch.list_namespaced_job, "default", **kwargs):
            log("Event: {} {}".format(event["type"], event["object"].metadata.name))
            status = event["object"].status

            if status.succeeded is not None:
                log("Job {} finished with status {}".format(job_name, status))
                break
            if event["type"] == "DELETED":
                log("ERROR: Job {} has been deleted!".format(job_name))
                break

            if status.failed == 1:
                log("ERROR: Job {} has failed! Cleaning up job and associated pods.".format(job_name))
                delete_options = self.__kube_client.V1DeleteOptions(propagation_policy="Foreground")
                self.__batch.delete_namespaced_job(name=job_name, namespace="default",
                                                   body=delete_options)
                break


def deserialize_to_json(msg_bytes):
    """
    Decode the message to a python object. Just let the exception
    propagate if decoding fails
    :param msg_bytes: message to decode
    :return: the job spec as json
    """
    return json.loads(msg_bytes.decode("utf8"))
