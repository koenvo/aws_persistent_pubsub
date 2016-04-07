__author__ = 'koen'

import json

import os
import yaml
import logging
import logging.config
import datetime

import moto
from mock import MagicMock

from unittest import TestCase

path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logging.yaml")
logging_conf = yaml.load(file(path, 'r'))

logging.config.dictConfig(logging_conf)

logger = logging.getLogger(__package__)

from aws_persistent_pubsub import PubSub


def patch_moto():
    # TODO: Need to create PR for this.
    def _fix_message(topic, message):
        now = datetime.datetime.now()
        message = json.dumps(dict(Message=message,
                                  TopicArn=topic.arn,
                                  Timestamp=now.strftime("%Y-%m-%dT%H:%M:%S.%fZ")))
        return message
    import moto.sns.models
    def patched_publish(self, message, message_id):
        if self.protocol == 'sqs':
            queue_name = self.endpoint.split(":")[-1]
            region = self.endpoint.split(":")[3]

            message = _fix_message(self.topic, message)
            moto.sns.models.sqs_backends[region].send_message(queue_name, message)
        elif self.protocol in ['http', 'https']:
            post_data = self.get_post_data(message, message_id)
            moto.sns.models.requests.post(self.endpoint, data=post_data)
    moto.sns.models.Subscription.publish = patched_publish

patch_moto()

class TestInit(TestCase):
    def setUp(self):
        self.sns_mock = moto.mock_sns()
        self.sns_mock.start()

        self.sqs_mock = moto.mock_sqs()
        self.sqs_mock.start()

    def tearDown(self):
        self.sns_mock.stop()
        self.sqs_mock.stop()

    def test_init(self):
        video_upload = MagicMock()

        app = PubSub("SomeSource", region_name="eu-west-1")
        # decorate video_upload
        app.listen("Profile.Update", ["Completed"])(video_upload)

        app.register_aws_resources()

        app.emit(process="Profile.Update",
                 event="Completed",
                 target="user:1298")

        app.poll()

        args, kwargs = video_upload.call_args
        assert kwargs["source"] == "SomeSource"
        assert kwargs["target"] == "user:1298"
        assert kwargs["event"] == "Completed"

    def test_listen(self):
        video_upload = MagicMock()

        app1 = PubSub("StateMachine1", region_name="eu-west-1")
        app1.listen("Image.Upload", ["Completed"])(video_upload)
        app1.register_aws_resources()

        app2 = PubSub("StateMachine2", region_name="eu-west-1")
        app2.register_aws_resources()

        app2.emit(process="Image.Upload",
                  event="Completed",
                  target="nice_kitten.png")

        app1.poll()

        args, kwargs = video_upload.call_args
        assert kwargs["source"] == "StateMachine2"
        assert kwargs["target"] == "nice_kitten.png"
        assert kwargs["event"] == "Completed"