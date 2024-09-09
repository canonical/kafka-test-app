#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.
import asyncio
import logging
import time

import pytest
from helpers import (
    fetch_action_get_credentials,
    fetch_action_start_process,
    fetch_action_stop_process,
    get_kafka_app_database_relation_data,
)
from pymongo import MongoClient
from pytest_operator.plugin import OpsTest

KAFKA = "kafka"
ZOOKEEPER = "zookeeper"
MONGODB = "mongodb"
CONSUMER = "kafka-consumer"
PRODUCER = "kafka-producer"
TLS_NAME = "self-signed-certificates"
DATA_INTEGRATOR = "data-integrator"
DATA_INTEGRATOR_PRODUCER = "data-integrator-producer"
DATA_INTEGRATOR_CONSUMER = "data-integrator-consumer"


logger = logging.getLogger(__name__)


@pytest.mark.abort_on_fail
async def test_deploy_charms(ops_test: OpsTest, kafka_app_charm):
    """Deploy both charms (application and database) to use in the tests."""
    # deploy kafka and zookeeper
    await asyncio.gather(
        ops_test.model.deploy(
            ZOOKEEPER,
            channel="3/edge",
            application_name=ZOOKEEPER,
            num_units=1,
            series="jammy",
        ),
        ops_test.model.deploy(
            KAFKA,
            channel="3/edge",
            application_name=KAFKA,
            num_units=1,
            series="jammy",
        ),
    )

    await ops_test.model.wait_for_idle(apps=[ZOOKEEPER, KAFKA], timeout=3000)

    assert ops_test.model.applications[KAFKA].status == "blocked"
    assert ops_test.model.applications[ZOOKEEPER].status == "active"

    await ops_test.model.add_relation(KAFKA, ZOOKEEPER)
    await ops_test.model.wait_for_idle(
        apps=[KAFKA, ZOOKEEPER], status="active", timeout=1000, idle_period=30
    )

    # deploy one producer and one consumer
    # todo add configuration in place!
    consumer_config = {"role": "consumer", "num_messages": 20}
    producer_config = {"role": "producer", "num_messages": 20}

    await asyncio.gather(
        ops_test.model.deploy(
            kafka_app_charm,
            application_name=CONSUMER,
            num_units=1,
            series="jammy",
            config=consumer_config,
        ),
        ops_test.model.deploy(
            kafka_app_charm,
            application_name=PRODUCER,
            num_units=1,
            series="jammy",
            config=producer_config,
        ),
    )

    await ops_test.model.wait_for_idle(apps=[CONSUMER, PRODUCER], timeout=1000, status="active")
    assert ops_test.model.applications[KAFKA].status == "active"
    assert ops_test.model.applications[ZOOKEEPER].status == "active"


@pytest.mark.abort_on_fail
async def test_producer_and_consumer_charms(ops_test: OpsTest, kafka_app_charm):
    """Add relation and start consumer and producers."""
    await ops_test.model.add_relation(KAFKA, PRODUCER)
    await ops_test.model.add_relation(KAFKA, CONSUMER)
    await ops_test.model.wait_for_idle(
        apps=[KAFKA, CONSUMER, PRODUCER], idle_period=60, status="active"
    )

    await ops_test.model.applications[KAFKA].remove_relation(
        f"{PRODUCER}:kafka-cluster", f"{KAFKA}:kafka-client"
    )
    await ops_test.model.wait_for_idle(apps=[KAFKA, PRODUCER])
    await ops_test.model.applications[KAFKA].remove_relation(
        f"{CONSUMER}:kafka-cluster", f"{KAFKA}:kafka-client"
    )
    await ops_test.model.wait_for_idle(apps=[KAFKA, CONSUMER])


@pytest.mark.abort_on_fail
async def test_producer_and_consumer_charms_with_actions(ops_test: OpsTest, kafka_app_charm):
    """Use the action to run producer and consumer."""
    topic_name = "topic_0"
    consumer_config = {"role": "consumer", "num_messages": "30", "topic_name": topic_name}
    producer_config = {"role": "producer", "num_messages": "30", "topic_name": topic_name}

    await ops_test.model.applications[PRODUCER].set_config(producer_config)
    await ops_test.model.wait_for_idle(apps=[PRODUCER], idle_period=10)

    await ops_test.model.applications[CONSUMER].set_config(consumer_config)
    await ops_test.model.wait_for_idle(apps=[CONSUMER], idle_period=10)

    config_consumer = {"topic-name": topic_name, "extra-user-roles": "consumer"}
    config_producer = {"topic-name": topic_name, "extra-user-roles": "producer"}

    await asyncio.gather(
        ops_test.model.deploy(
            DATA_INTEGRATOR,
            application_name=DATA_INTEGRATOR_CONSUMER,
            channel="edge",
            num_units=1,
            series="jammy",
            config=config_consumer,
        ),
        ops_test.model.deploy(
            DATA_INTEGRATOR,
            application_name=DATA_INTEGRATOR_PRODUCER,
            channel="edge",
            num_units=1,
            series="jammy",
            config=config_producer,
        ),
    )

    await ops_test.model.wait_for_idle(
        apps=[DATA_INTEGRATOR_PRODUCER, DATA_INTEGRATOR_CONSUMER], status="blocked", idle_period=10
    )

    # test the active/waiting status for relation
    await ops_test.model.wait_for_idle(
        apps=[KAFKA, DATA_INTEGRATOR_CONSUMER, DATA_INTEGRATOR_PRODUCER], idle_period=40
    )

    await ops_test.model.add_relation(KAFKA, DATA_INTEGRATOR_PRODUCER)
    await ops_test.model.wait_for_idle(apps=[KAFKA, DATA_INTEGRATOR_PRODUCER])

    await ops_test.model.add_relation(KAFKA, DATA_INTEGRATOR_CONSUMER)
    await ops_test.model.wait_for_idle(apps=[KAFKA, DATA_INTEGRATOR_CONSUMER])

    # get credential for Kafka
    producer_credentials = await fetch_action_get_credentials(
        ops_test.model.applications[DATA_INTEGRATOR_PRODUCER].units[0]
    )

    # get credential for Kafka
    consumer_credentials = await fetch_action_get_credentials(
        ops_test.model.applications[DATA_INTEGRATOR_CONSUMER].units[0]
    )
    logger.info(f"Producer credentials: {producer_credentials}")
    logger.info(f"Consumer credentials: {consumer_credentials}")
    # check for consumed messages in logs
    res_producer = await fetch_action_start_process(
        ops_test.model.applications[PRODUCER].units[0],
        "start-process",
        producer_credentials["kafka"]["endpoints"],
        producer_credentials["kafka"]["username"],
        producer_credentials["kafka"]["password"],
        producer_credentials["kafka"]["topic"],
    )

    logger.info(f"Res: {res_producer}")

    res_consumer = await fetch_action_start_process(
        ops_test.model.applications[CONSUMER].units[0],
        "start-process",
        consumer_credentials["kafka"]["endpoints"],
        consumer_credentials["kafka"]["username"],
        consumer_credentials["kafka"]["password"],
        consumer_credentials["kafka"]["topic"],
        consumer_group_prefix=consumer_credentials["kafka"]["consumer-group-prefix"],
    )

    logger.info(f"Res: {res_consumer}")

    time.sleep(20)

    res_stop_producer = await fetch_action_stop_process(
        ops_test.model.applications[PRODUCER].units[0],
        "stop-process",
    )
    logger.info(f"Stop result producer: {res_stop_producer}")
    res_stop_consumer = await fetch_action_stop_process(
        ops_test.model.applications[CONSUMER].units[0],
        "stop-process",
    )
    logger.info(f"Stop result consumer: {res_stop_consumer}")
    await ops_test.model.remove_application(DATA_INTEGRATOR_CONSUMER)
    await ops_test.model.remove_application(DATA_INTEGRATOR_PRODUCER)

    await ops_test.model.wait_for_idle(apps=[KAFKA])


@pytest.mark.abort_on_fail
async def test_deploy_mongodb_and_relate(ops_test: OpsTest, kafka_app_charm):
    """Deploy mongoDB, relate it with the kafka-app and dump messages."""
    # clean topic

    consumer_config = {"role": "consumer", "num_messages": "20", "topic_name": "topic_1"}
    producer_config = {"role": "producer", "num_messages": "20", "topic_name": "topic_1"}

    await ops_test.model.applications[PRODUCER].set_config(producer_config)
    await ops_test.model.wait_for_idle(apps=[PRODUCER], idle_period=10)

    await ops_test.model.applications[CONSUMER].set_config(consumer_config)
    await ops_test.model.wait_for_idle(apps=[CONSUMER], idle_period=10)

    await asyncio.gather(
        ops_test.model.deploy(
            MONGODB,
            channel="5/edge",
            application_name=MONGODB,
            num_units=1,
            series="jammy",
        ),
    )
    await ops_test.model.wait_for_idle(apps=[MONGODB], timeout=1000, status="active")
    await ops_test.model.add_relation(MONGODB, PRODUCER)
    await ops_test.model.wait_for_idle(apps=[MONGODB, PRODUCER])
    await ops_test.model.add_relation(MONGODB, CONSUMER)
    await ops_test.model.wait_for_idle(apps=[MONGODB, CONSUMER])

    # write messages to MongoDB
    await ops_test.model.add_relation(KAFKA, PRODUCER)
    await ops_test.model.wait_for_idle(apps=[KAFKA, PRODUCER], idle_period=60, status="active")

    await ops_test.model.add_relation(KAFKA, CONSUMER)
    await ops_test.model.wait_for_idle(apps=[KAFKA, CONSUMER], idle_period=60, status="active")

    # read messages to MongoDB

    mongodb_data = get_kafka_app_database_relation_data(
        unit_name=f"{PRODUCER}/0", model_full_name=ops_test.model_full_name
    )
    uris = mongodb_data["uris"]
    topic_name = mongodb_data["database"]
    logger.info(f"MongoDB uris: {uris}")
    logger.info(f"Topic: {topic_name}")
    try:
        client = MongoClient(
            uris,
            directConnection=False,
            connect=False,
            serverSelectionTimeoutMS=1000,
            connectTimeoutMS=2000,
        )
        db = client[topic_name]
        consumer_collection = db["consumer"]
        producer_collection = db["producer"]

        logger.info(f"Number of messages from consumer: {consumer_collection.count_documents({})}")
        logger.info(f"Number of messages from producer: {producer_collection.count_documents({})}")
        assert consumer_collection.count_documents({}) > 0
        assert producer_collection.count_documents({}) > 0
        assert consumer_collection.count_documents({}) == producer_collection.count_documents({})

        client.close()
    except Exception as e:
        logger.error("Cannot connect to MongoDB collection.")
        raise e

    # remove relation between kafka cluster and producer and consumer.
    await ops_test.model.applications[KAFKA].remove_relation(
        f"{PRODUCER}:kafka-cluster", f"{KAFKA}:kafka-client"
    )
    await ops_test.model.wait_for_idle(apps=[KAFKA, PRODUCER])
    await ops_test.model.applications[KAFKA].remove_relation(
        f"{CONSUMER}:kafka-cluster", f"{KAFKA}:kafka-client"
    )
    await ops_test.model.wait_for_idle(apps=[KAFKA, CONSUMER])

    # drop relation with MongoDB

    await ops_test.model.applications[MONGODB].remove_relation(
        f"{PRODUCER}:database", f"{MONGODB}:database"
    )
    await ops_test.model.wait_for_idle(apps=[MONGODB, PRODUCER])
    await ops_test.model.applications[MONGODB].remove_relation(
        f"{CONSUMER}:database", f"{MONGODB}:database"
    )
    await ops_test.model.wait_for_idle(apps=[MONGODB, CONSUMER])


@pytest.mark.abort_on_fail
async def test_tls(ops_test: OpsTest, kafka_app_charm):
    tls_config = {"ca-common-name": "kafka"}

    # FIXME (certs): Unpin the revision once the charm is fixed
    await asyncio.gather(
        ops_test.model.deploy(
            TLS_NAME, channel="edge", config=tls_config, series="jammy", revision=163
        ),
    )

    await ops_test.model.wait_for_idle(
        apps=[KAFKA, ZOOKEEPER, TLS_NAME], timeout=1800, status="active"
    )
    assert ops_test.model.applications[TLS_NAME].status == "active"

    logger.info("Relate Zookeeper to TLS")
    await ops_test.model.add_relation(TLS_NAME, ZOOKEEPER)
    await ops_test.model.wait_for_idle(apps=[TLS_NAME, ZOOKEEPER], idle_period=40, status="active")

    logger.info("Relate Kafka to TLS")
    await ops_test.model.add_relation(TLS_NAME, f"{KAFKA}:certificates")
    await ops_test.model.wait_for_idle(apps=[TLS_NAME, KAFKA], idle_period=40, status="active")

    logger.info("Relate Producer to TLS")
    await ops_test.model.add_relation(TLS_NAME, PRODUCER)
    await ops_test.model.wait_for_idle(apps=[TLS_NAME, PRODUCER], idle_period=10, status="active")

    logger.info("Relate Consumer to TLS")
    await ops_test.model.add_relation(TLS_NAME, CONSUMER)
    await ops_test.model.wait_for_idle(apps=[TLS_NAME, CONSUMER], idle_period=10, status="active")

    consumer_config = {"role": "consumer", "num_messages": "20", "topic_name": "topic_2"}
    producer_config = {"role": "producer", "num_messages": "20", "topic_name": "topic_2"}

    await ops_test.model.applications[PRODUCER].set_config(producer_config)
    await ops_test.model.wait_for_idle(apps=[PRODUCER], idle_period=10)

    await ops_test.model.applications[CONSUMER].set_config(consumer_config)
    await ops_test.model.wait_for_idle(apps=[CONSUMER], idle_period=10)

    # relate to mongodb
    await ops_test.model.wait_for_idle(apps=[MONGODB], timeout=1000, status="active")
    await ops_test.model.add_relation(MONGODB, PRODUCER)
    await ops_test.model.wait_for_idle(apps=[MONGODB, PRODUCER], idle_period=10)
    await ops_test.model.add_relation(MONGODB, CONSUMER)
    await ops_test.model.wait_for_idle(apps=[MONGODB, CONSUMER], idle_period=10)

    # relate producer and consumer
    await ops_test.model.add_relation(KAFKA, PRODUCER)
    await ops_test.model.wait_for_idle(apps=[KAFKA, PRODUCER], idle_period=60, status="active")

    await ops_test.model.add_relation(KAFKA, CONSUMER)
    await ops_test.model.wait_for_idle(apps=[KAFKA, CONSUMER], idle_period=60, status="active")

    # Check messages in mongodb
    mongodb_data = get_kafka_app_database_relation_data(
        unit_name=f"{PRODUCER}/0", model_full_name=ops_test.model_full_name
    )
    uris = mongodb_data["uris"]
    topic_name = mongodb_data["database"]
    logger.info(f"MongoDB uris: {uris}")
    logger.info(f"Topic: {topic_name}")
    try:
        client = MongoClient(
            uris,
            directConnection=False,
            connect=False,
            serverSelectionTimeoutMS=1000,
            connectTimeoutMS=2000,
        )
        db = client[topic_name]
        consumer_collection = db["consumer"]
        producer_collection = db["producer"]

        logger.info(f"Number of messages from consumer: {consumer_collection.count_documents({})}")
        logger.info(f"Number of messages from producer: {producer_collection.count_documents({})}")
        assert consumer_collection.count_documents({}) > 0
        assert producer_collection.count_documents({}) > 0
        assert consumer_collection.count_documents({}) == producer_collection.count_documents({})

        client.close()
    except Exception as e:
        logger.error("Cannot connect to MongoDB collection.")
        raise e

    await ops_test.model.applications[KAFKA].remove_relation(
        f"{PRODUCER}:kafka-cluster", f"{KAFKA}:kafka-client"
    )
    await ops_test.model.wait_for_idle(apps=[KAFKA, PRODUCER])
    await ops_test.model.applications[KAFKA].remove_relation(
        f"{CONSUMER}:kafka-cluster", f"{KAFKA}:kafka-client"
    )
    await ops_test.model.wait_for_idle(apps=[KAFKA, CONSUMER])
