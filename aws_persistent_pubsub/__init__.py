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

class WrongNamespace(Exception):
    pass

def parse_utc_timestamp_to_local_datetime(string):
    utc = datetime.datetime.strptime(string, "%Y-%m-%dT%H:%M:%S.%fZ")
    utc = utc.replace(tzinfo=tz.tzutc())

    return utc.astimezone(tz.tzlocal())


class PubSub(object):
    def __init__(self, namespace, process, ack_on_exception=False, **aws_kwargs):
        self._ack_on_exception = ack_on_exception
        self._namespace = namespace

        # create own process queue when missing
        self._process = process
        self._process_queue = None
        self._source_process_subscriptions = {}
        self._source_process_to_topic_arn_map = {}
        self._source_process_to_subscription_arn_map = {}

        self._sqs_conn = boto.sqs.connect_to_region(**aws_kwargs)
        self._sns_conn = boto.sns.connect_to_region(**aws_kwargs)

        # needed for sending messages
        self._source_process_to_topic_arn_map = None

        self._refresh_source_process_to_topic_arn_map()

    def _arn_to_process(self, arn):
        namespace, process = arn.split(":")[-1].split("__")
        if namespace != self._namespace:
            raise WrongNamespace("Arn '{}' does not belong to namespace '{}'".format(
                arn, namespace
            ))
        return process.replace("_", ".")

    def _belongs_to_namespace(self, arn):
        try:
            self._arn_to_process(arn)
        except WrongNamespace:
            return False
        else:
            return True

    def _process_to_arn_friendly_name(self, process):
        """Add namespace and replace dot with underscore."""
        return "{namespace}__{process}".format(
            namespace=self._namespace, process=process.replace(".", "_")
        )

    @staticmethod
    def _attach_wildcard_policy(queue):
        sid = "ReceiveMessagesFromAllTopics-{}".format(
            queue.arn.split(":")[-1]
        )
        policy = {
            'Version': '2008-10-17',
            'Statement': [{
                'Sid': sid,
                'Effect': 'Allow',
                'Principal': {
                    'AWS': '*'
                },
                'Action': 'SQS:SendMessage',
                'Resource': queue.arn
            }]
        }
        queue.set_attribute('Policy', json.dumps(policy))

    def _refresh_source_process_to_topic_arn_map(self):
        self._source_process_to_topic_arn_map = self._get_source_process_to_topic_arn_map()

    def _create_queue(self, queue_name):
        """Create a queue and attach policy to allow SNS messages from everywhere."""
        process_queue = self._sqs_conn.create_queue(self._process_to_arn_friendly_name(queue_name))
        self._attach_wildcard_policy(process_queue)
        return process_queue

    def _create_topic(self, process):
        topic = self._sns_conn.create_topic(self._process_to_arn_friendly_name(process))["CreateTopicResponse"]["CreateTopicResult"]
        return topic['TopicArn']

    def _create_subscription(self, process, queue):
        """Create subscription to SNS. Don't use subscribe_sqs_queue because this function will attach
           a policy, but we set a wider policy on the queue in _create_queue. """
        subscription = self._sns_conn.subscribe(
            self._source_process_to_topic_arn_map[process], 'sqs', queue.arn
        )["SubscribeResponse"]["SubscribeResult"]
        return subscription['SubscriptionArn']


    def _get_process_queue(self, process):
        process_queue = self._sqs_conn.get_queue(process)
        if process_queue is None:
            process_queue = self._create_queue(process)

        process_queue.set_message_class(boto.sqs.message.RawMessage)
        return process_queue

    def _get_source_process_to_topic_arn_map(self):
        _topics = []

        next_token = None
        while True:
            response = self._sns_conn.get_all_topics(
                next_token=next_token
            )["ListTopicsResponse"]["ListTopicsResult"]

            _topics.extend([topic for topic in response['Topics']
                            if self._belongs_to_namespace(topic['TopicArn'])])

            next_token = response['NextToken']
            if next_token is None:
                break
        return {self._arn_to_process(topic['TopicArn']): topic['TopicArn'] for topic in _topics}

    def _get_topic_subscriptions(self, topic_arn):
        _subscriptions = []

        next_token = None
        while True:
            response = self._sns_conn.get_all_subscriptions_by_topic(
                topic=topic_arn,
                next_token=next_token
            )["ListSubscriptionsByTopicResponse"]["ListSubscriptionsByTopicResult"]

            _subscriptions.extend(response['Subscriptions'])

            next_token = response["NextToken"]
            if next_token is None:
                break

        return _subscriptions

    def _get_source_process_to_subscription_arn_map(self, process):
        """Return all subscriptions that send data to `process`.
           Have to retrieve the subscriptions on topic base: https://forums.aws.amazon.com/thread.jspa?threadID=219168
        """
        _subscriptions = []

        for topic_arn in self._source_process_to_topic_arn_map.values():
            _subscriptions.extend(self._get_topic_subscriptions(topic_arn))

        # unsubscribe all
        # map(self._sns_conn.unsubscribe, [sub['SubscriptionArn'] for sub in _subscriptions])

        # in a loop for readability
        ret = {}
        for _subscription in _subscriptions:
            target_process = self._arn_to_process(_subscription['Endpoint'])
            if target_process == process:
                source_process = self._arn_to_process(_subscription['TopicArn'])
                ret[source_process] = _subscription['SubscriptionArn']
        return ret

    def _handle_message(self, handler, event, target, source, extra, trigger_datetime):
        handler(event=event,
                target=target,
                source=source,
                extra=extra,
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

    def emit(self, process, event, target, extra=None):
        message = dict(
            event=event,
            target=target,
            source=self._process,
            extra=extra
        )

        # raise exception when noone is listening to a topic
        if process not in self._source_process_to_topic_arn_map:
            self._refresh_source_process_to_topic_arn_map()
            if process not in self._source_process_to_topic_arn_map:
                raise Exception("No one is listening for '{}'".format(process))

        self._sns_conn.publish(topic=self._source_process_to_topic_arn_map[process], message=json.dumps(message))

    def register_aws_resources(self):
        """Should be called after all listen decorators."""
        self._process_queue = self._get_process_queue(self._process)

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
            source_process = self._arn_to_process(body['TopicArn'])

            exception_raised = False
            if source_process in self._source_process_subscriptions:
                trigger_datetime = parse_utc_timestamp_to_local_datetime(body['Timestamp'])
                _message = json.loads(body['Message'])

                event = _message['event']
                if isinstance(event, list):
                    event = tuple(event)

                if event in self._source_process_subscriptions[source_process]:
                    for handler in self._source_process_subscriptions[source_process][event]:
                        try:
                            self._handle_message(handler,
                                                 event=event,
                                                 target=_message['target'],
                                                 source=_message['source'],
                                                 extra=_message['extra'],
                                                 trigger_datetime=trigger_datetime)
                        except Exception as exc:
                            logger.error(u"Handler '{}' raised an exception: '{}'".format(
                                handler, exc
                            ))
                            exception_raised = True
                else:
                    logger.debug(u"Dropped message '{}'. Event not wanted.".format(body))

            else:
                logger.warning(u"Dropped message '{}'. Didn't ask for it.".format(body))

            if self._ack_on_exception or not exception_raised:
                message.delete()