import json
from unittest import TestCase
from unittest.mock import Mock

from kubernetes import client

from kub.job import QueueConsumer, JobSpecCreator, JobExecutor


class TestQueueConsumer(TestCase):
    def test_listen_and_consume_sets_up_queue_and_starts_listening(self):
        # Arrange
        def cb():
            pass

        queue_name = "test"
        prefetch_count = 1
        mock_connection = Mock()
        mock_channel = Mock()
        mock_connection.channel.return_value = mock_channel

        # Act
        QueueConsumer(mock_connection, queue_name, prefetch_count, cb).listen_and_consume()

        # Assert
        self.assertTrue(mock_connection.channel.called)
        mock_channel.queue_declare.assert_called_with(queue=queue_name, durable=True)
        mock_channel.basic_qos.assert_called_with(prefetch_count=prefetch_count)
        mock_channel.basic_consume.assert_called_with(cb, queue=queue_name)
        self.assertTrue(mock_channel.start_consuming.called)


class TestJobSpecCreator(TestCase):
    def test_create_builds_correct_job_resource(self):
        # Arrange
        name = "test"
        image = "alpine"
        command = ["echo", "test"]
        args = []
        labels = {}

        # Act
        job = JobSpecCreator(client).create(name, image, command, args, labels)

        # Assert
        self.assertEqual(job.api_version, "batch/v1")
        self.assertEqual(job.metadata.name, name)
        self.assertEqual(job.spec.template.metadata.name, name)
        container_spec = job.spec.template.spec.containers[0]
        self.assertEqual(container_spec.name, name)
        self.assertEqual(container_spec.image, image)
        self.assertEqual(container_spec.command, command)


class TestJobExecutor(TestCase):
    def test_execute_job_sync(self):
        # Arrange
        name = "test"
        image = "alpine"
        command = ["echo"]
        args = ["hello"]
        msg = '{{"name": "{}", "command": {}, "image": "{}", "args": {}}}'.format(name, json.dumps(command), image,
                                                                                  json.dumps(args))
        print(msg)
        client = Mock()
        batch = Mock()
        batch.list_namespaced_job.return_value = MockResponse()
        client.BatchV1Api.return_value = batch
        channel = Mock()
        creator = Mock()
        exec = JobExecutor(client, creator)

        # Act
        exec.execute_job_sync(channel, MockMethod(), [], msg.encode())

        # Assert
        creator.create.assert_called_with(AnyStringWith(name), image, command, args, {})


class MockResponse:
    def release_conn(self):
        pass

    def read_chunked(self, decode_content):
        return []

    def close(self):
        pass

    def __iter__(self):
        return self

    def __next__(self):
        pass


class MockMethod:
    def delivery_tag(self):
        pass


class AnyStringWith(str):
    def __eq__(self, other):
        return self in other
