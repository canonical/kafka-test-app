#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

import glob
import os
import platform

import pytest
from pytest_operator.plugin import OpsTest

TEST_BASE = "ubuntu@24.04"


def pytest_addoption(parser):
    """Defines pytest parsers."""
    parser.addoption("--kafka", action="store", help="Kafka version", default="3")
    parser.addoption(
        "--base", action="store", help="Charm base to use for tests.", default="ubuntu@24.04"
    )


@pytest.fixture(scope="module")
def kafka_version(request: pytest.FixtureRequest) -> int:
    """Return the Kafka version used for tests."""
    val = f'{request.config.getoption("--kafka")}' or "3"
    if val not in ("3", "4"):
        raise Exception("Unknown Kafka version, valid options are 3 and 4")

    return int(val)


@pytest.fixture(scope="module")
def base(request: pytest.FixtureRequest) -> str:
    """Return the charm base used for tests."""
    return f'{request.config.getoption("--base")}'


@pytest.fixture(scope="module")
def series(base: str) -> str:
    """Return the mapped series value for the charm base."""
    return {"ubuntu@22.04": "jammy", "ubuntu@24.04": "noble"}.get(base, "noble")


@pytest.fixture(scope="module")
async def kafka_app_charm(ops_test: OpsTest, base: str):
    """Build the application charm."""
    if "CI" not in os.environ:
        charm_path = "."
        charm = await ops_test.build_charm(charm_path)
        return charm

    processor = platform.processor()
    architecture = "arm64" if processor == "aarch64" else "amd64"
    match = glob.glob(f"*{base}-{architecture}.charm")
    if not match:
        raise RuntimeError(f"Can not find appropriate charm file for {base=} {architecture=}")

    return f"./{match[0]}"
