import os
import random
import time
import timeit
from subprocess import PIPE, Popen

import pytest
import requests


# Ensure that test users exist in environment
@pytest.fixture(scope="session", autouse=True)
def ch_test_users():
    test_users = [
        os.environ.setdefault(f'DBT_TEST_USER_{x}', f'dbt_test_user_{x}') for x in range(1, 4)
    ]
    yield test_users


# This fixture is for customizing tests that need overrides in adapter
# repos. Example in dbt.tests.adapter.basic.test_base.
@pytest.fixture(scope="session")
def test_config(ch_test_users):
    yield {
        'driver': 'native',
        'host': 'gateway.aws-ap-southeast-1.bytehouse.cloud',
        'port': 19000,
        'user': 'bytehouse',
        'password': '',
        'db_engine': '',
        'secure': True,
        'cluster_mode': False,
    }


# The profile dictionary, used to write out profiles.yml
# dbt will supply a unique schema per test, so we do not specify 'schema' here
@pytest.fixture(scope="class")
def dbt_profile_target(test_config):
    return {
        'type': 'bytehouse',
        'threads': 1,
        'driver': test_config['driver'],
        'host': test_config['host'],
        'user': test_config['user'],
        'password': test_config['password'],
        'port': test_config['port'],
        'database_engine': test_config['db_engine'],
        'cluster_mode': test_config['cluster_mode'],
        'secure': test_config['secure'],
        'check_exchange': False,
    }


@pytest.fixture(scope="class")
def prefix():
    return f"dbt_bytehouse_{random.randint(1000, 9999)}"


@pytest.fixture(scope="class")
def unique_schema(request, prefix) -> str:
    test_file = request.module.__name__.split(".")[-1]
    return f"{prefix}_{test_file}_{int(time.time() * 1000)}"


def run_cmd(cmd):
    with Popen(cmd, stdout=PIPE, stderr=PIPE) as popen:
        stdout, stderr = popen.communicate()
        return popen.returncode, stdout, stderr


def is_responsive(url):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return True
    except requests.exceptions.ConnectionError:
        return False


def wait_until_responsive(check, timeout, pause, clock=timeit.default_timer):
    ref = clock()
    now = ref
    while (now - ref) < timeout:
        time.sleep(pause)
        if check():
            return
        now = clock()
    raise Exception("Timeout reached while waiting on service!")
