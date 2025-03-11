#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2025 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# A simple example demonstrating use of JSONDeserializer.

import argparse
import os

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.rules.encryption.awskms.aws_driver import AwsKmsDriver

from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from dotenv import load_dotenv
from confluent_kafka.schema_registry.rules.encryption.encrypt_executor import \
    FieldEncryptionExecutor


class User(object):
    """
    User record

    Args:
        name (str): User's name
        favorite_number (int): User's favorite number
        favorite_color (str): User's favorite color
    """

    def __init__(self, name=None, favorite_number=None, favorite_color=None):
        self.name = name
        self.favorite_number = favorite_number
        self.favorite_color = favorite_color


def dict_to_user(obj, ctx):
    """
    Converts object literal(dict) to a User instance.

    Args:
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
        obj (dict): Object literal(dict)
    """

    if obj is None:
        return None

    return User(name=obj['name'],
                favorite_number=obj['favorite_number'],
                favorite_color=obj['favorite_color'])


def get_env(var_name: str) -> str:
    return os.getenv(var_name)


def main():
    load_dotenv()

    # Register the KMS drivers and the field-level encryption executor
    AwsKmsDriver.register()
    FieldEncryptionExecutor.register()

    bootstrap_server = get_env('BOOTSTRAP_SERVER_URL')
    kafka_api_key=get_env('KAFKA_KEY')
    kafka_api_secret=get_env('KAFKA_SECRET')
    kafka_topic=get_env('TOPIC_NAME')

    sr_server=get_env('SR_SERVER_URL')
    sr_api_key=get_env('SR_API_KEY')
    sr_api_secret=get_env('SR_API_SECRET')

    topic = kafka_topic

    # When using Data Contract rules, a schema should not be passed to the
    # JSONDeserializer. The schema is fetched from the Schema Registry.
    schema_str = None

    # Load the environment variables for Schema Registry and KMS
    sr_server = get_env('SR_SERVER_URL')
    sr_api_key = get_env('SR_API_KEY')
    sr_api_secret = get_env('SR_API_SECRET')

    sr_conf = {
        'url': sr_server,
        'basic.auth.user.info': f"{sr_api_key}:{sr_api_secret}"
    }
    schema_registry_client = SchemaRegistryClient(sr_conf)

    json_deserializer = JSONDeserializer(schema_str,
                                         dict_to_user,
                                         schema_registry_client)

    consumer_conf ={
        'bootstrap.servers': bootstrap_server,
        'sasl.mechanisms': 'PLAIN',
        'security.protocol': 'SASL_SSL',
        'sasl.username': kafka_api_key,
        'sasl.password': kafka_api_secret,
        'group.id': 'python_csfle_example',
        'enable.metrics.push': False

    }

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            user = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))

            if user is not None:
                print("User record: name: {}\n"
                      "\tfavorite_number: {}\n"
                      "\tfavorite_color: {}\n"
                      .format(user.name,
                              user.favorite_number,
                              user.favorite_color))
        except KeyboardInterrupt:
            break

    consumer.close()


if __name__ == '__main__':

    main()
