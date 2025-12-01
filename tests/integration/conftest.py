#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.


import pytest
from pytest_operator.plugin import OpsTest


def pytest_addoption(parser):
    """Defines pytest parsers."""
    parser.addoption("--kafka", action="store", help="Kafka version", default="3")


@pytest.fixture(scope="module")
def kafka_version(request: pytest.FixtureRequest) -> int:
    """Returns the Kafka version used for tests`."""
    val = f'{request.config.getoption("--kafka")}' or "3"
    if val not in ("3", "4"):
        raise Exception("Unknown Kafka version, valid options are 3 and 4")

    return int(val)


@pytest.fixture(scope="module")
async def kafka_app_charm(ops_test: OpsTest):
    """Build the application charm."""
    charm_path = "."
    charm = await ops_test.build_charm(charm_path)
    return charm
