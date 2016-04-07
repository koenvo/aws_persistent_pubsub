__author__ = 'koen'

import logging
from collections import defaultdict
import datetime
from dateutil import tz

import boto.sqs
import boto.sqs.message
import boto.sns
import json

logger = logging.getLogger(__package__)


def parse_utc_timestamp_to_local_datetime(string):
    utc = datetime.datetime.strptime(string, "%Y-%m-%dT%H:%M:%S.%fZ")
    utc = utc.replace(tzinfo=tz.tzutc())

    return utc.astimezone(tz.tzlocal())


def arn_to_process(arn):
    return arn.split(":")[-1].replace("_", ".")


class PubSub(object):
    def __init__(self, process, **aws_kwargs):
        # create own process queue when missing
        self._process = process
        self._process_queue = None
        self._source_process_subscriptions = {}
        self._source_process_to_topic_arn_map = {}
        self._source_process_to_subscription_arn_map = {}

        self._aws_kwargs = aws_kwargs
        self._sqs_conn = None
        self._sns_conn = None

    def _create_topic(self, process):
        topic = self._sns_conn.create_topic(process.replace(".", "_"))["CreateTopicResponse"]["CreateTopicResult"]
        return topic['TopicArn']

    def _create_subscription(self, process, queue):
        subscription = self._sns_conn.subscribe_sqs_queue(
            self._source_process_to_topic_arn_map[process],
            queue
        )["SubscribeResponse"]["SubscribeResult"]
        return subscription['SubscriptionArn']

    def _get_process_queue(self, process):
        process_queue = self._sqs_conn.get_queue(process)
        if process_queue is None:
            process_queue = self._sqs_conn.create_queue(process)

        process_queue.set_message_class(boto.sqs.message.RawMessage)
        return process_queue

    def _get_source_process_to_topic_arn_map(self):
        _topics = []

        next_token = None
        while True:
            response = self._sns_conn.get_all_topics(
                next_token=next_token
            )["ListTopicsResponse"]["ListTopicsResult"]

            _topics.extend(response['Topics'])

            next_token = response['NextToken']
            if next_token is None:
                break
        return {arn_to_process(topic['TopicArn']): topic['TopicArn'] for topic in _topics}

    def _get_source_process_to_subscription_arn_map(self, process):
        """Return all subscriptions that send data to `process`."""
        _subscriptions = []

        next_token = None
        while True:
            response = self._sns_conn.get_all_subscriptions(
                next_token=next_token
            )["ListSubscriptionsResponse"]["ListSubscriptionsResult"]

            _subscriptions.extend(response['Subscriptions'])

            next_token = response["NextToken"]
            if next_token is None:
                break

        # in a loop for readability
        ret = {}
        for _subscription in _subscriptions:
            target_process = arn_to_process(_subscription['Endpoint'])
            if target_process == process:
                source_process = arn_to_process(_subscription['TopicArn'])
                ret[source_process] = _subscription['SubscriptionArn']
        return ret

    def _handle_message(self, handler, event, target, source, trigger_datetime):
        handler(event=event,
                target=target,
                source=source,
                trigger_datetime=trigger_datetime)

    def listen(self, source_process, events):
        """Subscribe to one or more events on process."""
        def _wrapper(fn):
            if source_process not in self._source_process_subscriptions:
                self._source_process_subscriptions[source_process] = defaultdict(set)

            for event in events:
                self._source_process_subscriptions[source_process][event].add(fn)
            return fn
        return _wrapper

    def emit(self, process, event, target):
        message = dict(
            event=event,
            target=target,
            source=self._process
        )

        # raise exception when noone is listening to a topic
        self._sns_conn.publish(topic=self._source_process_to_topic_arn_map[process], message=json.dumps(message))

    def register_aws_resources(self):
        """Should be called after all listen decorators."""
        self._sqs_conn = boto.sqs.connect_to_region(**self._aws_kwargs)
        self._sns_conn = boto.sns.connect_to_region(**self._aws_kwargs)

        self._process_queue = self._get_process_queue(self._process)

        self._source_process_to_topic_arn_map = self._get_source_process_to_topic_arn_map()
        self._source_process_to_subscription_arn_map = self._get_source_process_to_subscription_arn_map(self._process)

        registered_source_processes = set(self._source_process_subscriptions.keys())
        registered_topics = set(self._source_process_to_topic_arn_map.keys())
        registered_subscriptions = set(self._source_process_to_subscription_arn_map.keys())

        # create topics when they are missing
        source_processes_missing_topic = registered_source_processes - registered_topics
        for source_process in source_processes_missing_topic:
            self._source_process_to_topic_arn_map[source_process] = self._create_topic(source_process)

        # create subscriptions when missing
        source_processes_missing_subscription = registered_source_processes - registered_subscriptions
        for source_process in source_processes_missing_subscription:
            self._source_process_to_subscription_arn_map[source_process] = \
                self._create_subscription(source_process, self._process_queue)

        # remove all subscriptions where we don't listen to anymore
        stale_subscriptions = registered_subscriptions - registered_source_processes
        for stale_subscription in stale_subscriptions:
            self._sns_conn.unsubscribe(self._source_process_to_subscription_arn_map[stale_subscription])
            del self._source_process_to_subscription_arn_map[stale_subscription]

    def poll(self):
        logger.debug("Waiting for messages")
        messages = self._process_queue.get_messages(wait_time_seconds=10)
        logger.debug("Got {} messages".format(len(messages)))
        for message in messages:
            body = json.loads(message.get_body())
            source_process = arn_to_process(body['TopicArn'])

            if source_process in self._source_process_subscriptions:
                trigger_datetime = parse_utc_timestamp_to_local_datetime(body['Timestamp'])
                _message = json.loads(body['Message'])
                if _message['event'] in self._source_process_subscriptions[source_process]:
                    for handler in self._source_process_subscriptions[source_process][_message['event']]:
                        try:
                            self._handle_message(handler,
                                                 event=_message['event'],
                                                 target=_message['target'],
                                                 source=_message.get('source'),
                                                 trigger_datetime=trigger_datetime)
                        except Exception as exc:
                            logger.error(u"Handler '{}' raised an exception: '{}'".format(
                                handler, exc
                            ))
                else:
                    logger.debug(u"Dropped message '{}'. Event not wanted.".format(body))

            else:
                logger.warning(u"Dropped message '{}'. Didn't ask for it.".format(body))

            message.delete()