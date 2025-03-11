#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2024 Confluent Inc.
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


# A simple example demonstrating use of JSONSerializer.

import argparse
import os
from uuid import uuid4

from confluent_kafka.schema_registry.rules.encryption.encrypt_executor import \
    FieldEncryptionExecutor

from confluent_kafka.schema_registry.rules.encryption.awskms.aws_driver import \
    AwsKmsDriver
from six.moves import input

from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient, Rule, \
    RuleKind, RuleMode, RuleParams, Schema, RuleSet
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from dotenv import load_dotenv

import schema


class User(object):
    """
    User record

    Args:
        name (str): User's name

        favorite_number (int): User's favorite number

        favorite_color (str): User's favorite color
    """

    def __init__(self, name, favorite_number, favorite_color):
        self.name = name
        self.favorite_number = favorite_number
        self.favorite_color = favorite_color


def user_to_dict(user, ctx):
    """
    Returns a dict representation of a User instance for serialization.

    Args:
        user (User): User instance.

        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.

    Returns:
        dict: Dict populated with user attributes to be serialized.
    """

    return dict(name=user.name,
                favorite_number=user.favorite_number,
                favorite_color=user.favorite_color)


def delivery_report(err, msg):
    """
    Reports the success or failure of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    """

    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))

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
    kms_key_id=get_env('KMS_KEY_ID')
    kek_name=get_env('KEK_NAME')
    
    schema_registry_conf = {
        'url': sr_server,
        'basic.auth.user.info': f"{sr_api_key}:{sr_api_secret}"
    }
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    rule = Rule(
        "test-encrypt",
        "",
        RuleKind.TRANSFORM,
        RuleMode.WRITEREAD,
        "ENCRYPT",
        ["PII"],
        RuleParams({
            "encrypt.kek.name": kek_name,
            "encrypt.kms.type": "aws-kms",
            "encrypt.kms.key.id": kms_key_id
        }),
        None,
        None,
        "ERROR,NONE",
        False
    )

    subject = f"{kafka_topic}-value"
    schema_registry_client.register_schema(subject, Schema(
        schema.USER_SCHEMA,
        "JSON",
        [],
        None,
        RuleSet(None, [rule])
    ))

    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    rule_conf = None
    # KMS credentials can be passed as follows
    # rule_conf = {'secret.access.key': 'xxx', 'access.key.id': 'yyy'}
    # Alternatively, the KMS credentials can be set via environment variables
    json_serializer = JSONSerializer(schema.USER_SCHEMA,
                                     schema_registry_client,
                                     user_to_dict,
                                     conf=ser_conf,
                                     rule_conf=rule_conf)

    string_serializer = StringSerializer('utf_8')

     # Kafka producer configuration
    producer_conf = {
        'bootstrap.servers': bootstrap_server,
        'sasl.mechanisms': 'PLAIN',
        'security.protocol': 'SASL_SSL',
        'sasl.username': kafka_api_key,
        'sasl.password': kafka_api_secret,
        'enable.metrics.push': False
    }

    producer = Producer(producer_conf)

    print("Producing user records to topic {}. ^C to exit.".format(kafka_topic))
    while True:
        # Serve on_delivery callbacks from previous calls to produce()
        producer.poll(0.0)
        try:
            user_name = input("Enter name: ")
            user_favorite_number = int(input("Enter favorite number: "))
            user_favorite_color = input("Enter favorite color: ")
            user = User(name=user_name,
                        favorite_color=user_favorite_color,
                        favorite_number=user_favorite_number)
            producer.produce(topic=kafka_topic,
                             key=string_serializer(str(uuid4())),
                             value=json_serializer(user, SerializationContext(kafka_topic, MessageField.VALUE)),
                             on_delivery=delivery_report)
            print("Message produced")
        except KeyboardInterrupt:
            break
        except ValueError:
            print("Invalid input, discarding record...")
            continue

    print("\nFlushing records...")
    producer.flush()


if __name__ == '__main__':
    main()
