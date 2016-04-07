# -*- coding: utf-8 -*-
"""
AWS Persistent PubSub
-----------------------
Provides a pubsub system based on AWS SNS and SQS. Because messages are routed through
SQS they are persisted during disconnect of receiver.
"""

from setuptools import setup

setup(
    name="aws_persistent_pubsub",
    version="0.1",
    packages=["aws_persistent_pubsub"],
    author="Koen Vossen",
    author_email="info@koenvossen.nl",
    url='',
    license="MIT",
    description='Persistent PubSub on AWS infrastructure.',
    long_description=__doc__,
    classifiers=[],
    install_requires=["boto>=2.36.0"],
    tests_require=[],
    extras_require={}
)